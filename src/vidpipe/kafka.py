"""Kafka helpers shared by every worker.

run_worker implements the consume loop once:
  poll -> validate -> handler (with retries) -> publish result -> commit offset.
The offset is committed only after the result is published, so delivery is at-least-once;
handlers must therefore be idempotent. A message that keeps failing, or cannot be parsed,
goes to pipeline.errors and is committed so it cannot block the partition.
"""
import logging
import signal
import time

from confluent_kafka import Consumer, Producer
from pydantic import ValidationError

from vidpipe import db
from vidpipe.config import cfg
from vidpipe.schemas import ErrorEvent, Topics

log = logging.getLogger(__name__)

MAX_ORIGINAL_CHARS = 4000


def make_producer():
    return Producer({
        "bootstrap.servers": cfg.kafka_bootstrap,
        "enable.idempotence": True,
        "acks": "all",
    })


def make_consumer(group_id):
    return Consumer({
        "bootstrap.servers": cfg.kafka_bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        # Transcription can take a long time per message; allow up to 1 hour between polls.
        "max.poll.interval.ms": 3_600_000,
        "session.timeout.ms": 45_000,
    })


def publish(producer, topic, message):
    """Publish a message keyed by video_id (keeps one video's messages ordered)."""
    producer.produce(topic, key=message.video_id, value=message.model_dump_json())
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"could not deliver message to {topic}")


def _dead_letter(producer, stage, source_topic, video_id, run_id, exc, attempts, raw):
    event = ErrorEvent(
        video_id=video_id,
        run_id=run_id,
        stage=stage,
        source_topic=source_topic,
        error_type=type(exc).__name__,
        error=str(exc),
        attempts=attempts,
        original=raw[:MAX_ORIGINAL_CHARS] if raw else None,
    )
    publish(producer, Topics.PIPELINE_ERRORS, event)


def _record(run_id, video_id, stage, status, error=None):
    """Best effort: a bookkeeping failure must not hide the original error."""
    try:
        with db.connect() as conn:
            db.record_stage(conn, run_id, video_id, stage, status, error)
    except Exception:
        log.exception("could not record %s/%s for %s", stage, status, video_id)


def run_worker(*, stage, group_id, in_topic, in_model, out_topic, handler,
               max_attempts=2, retry_delay_s=5):
    """Consume in_topic forever. handler(message) returns the message to publish on out_topic."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    consumer = make_consumer(group_id)
    producer = make_producer()
    consumer.subscribe([in_topic])

    stopping = False

    def _stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    log.info("%s worker started: %s -> %s (group %s)", stage, in_topic, out_topic, group_id)

    try:
        while not stopping:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("kafka error: %s", msg.error())
                continue

            raw = msg.value().decode("utf-8", errors="replace")
            key = msg.key().decode() if msg.key() else "unknown"

            try:
                message = in_model.model_validate_json(raw)
            except ValidationError as exc:
                log.error("invalid message on %s (key=%s): %s", in_topic, key, exc)
                _dead_letter(producer, stage, in_topic, key, "unknown", exc, 0, raw)
                consumer.commit(message=msg, asynchronous=False)
                continue

            last_exc = None
            for attempt in range(1, max_attempts + 1):
                _record(message.run_id, message.video_id, stage, "started")
                try:
                    result = handler(message)
                    if result is not None and out_topic:
                        publish(producer, out_topic, result)
                    _record(message.run_id, message.video_id, stage, "succeeded")
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    log.exception("%s failed for %s (attempt %d/%d)",
                                  stage, message.video_id, attempt, max_attempts)
                    if attempt < max_attempts:
                        time.sleep(retry_delay_s * attempt)

            if last_exc is not None:
                _record(message.run_id, message.video_id, stage, "failed",
                        f"{type(last_exc).__name__}: {last_exc}")
                _dead_letter(producer, stage, in_topic, message.video_id, message.run_id,
                             last_exc, max_attempts, raw)

            consumer.commit(message=msg, asynchronous=False)
    finally:
        log.info("%s worker stopping", stage)
        consumer.close()
