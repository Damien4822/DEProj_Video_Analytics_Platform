"""Summarizer: timeline.ready -> local LLM -> short summary row -> summary.ready.

Reads the whole-video transcript written by the timeline builder and asks a local LLM (Ollama)
what is going on. The LLM call is the slow, costly part, so it is skipped when the transcript
and the model are the same as for the stored summary.

    python -m services.summarizer.worker    (run from the local/ folder)
"""
import json
import logging

from vidpipe import db, storage
from vidpipe.config import cfg
from vidpipe.kafka import run_worker
from vidpipe.models.llm import OllamaClient
from vidpipe.schemas import SummaryReady, TimelineReady, Topics
from vidpipe.summary import summarize

STAGE = "summary"
log = logging.getLogger(__name__)

llm = OllamaClient()


def handle(msg: TimelineReady):
    with db.connect() as conn:
        timeline = db.get_timeline(conn, msg.video_id)   # the latest transcript, not the message's
        existing = db.get_summary(conn, msg.video_id)
    if timeline is None:
        raise ValueError(f"no timeline stored for video {msg.video_id}")
    if (existing and existing["source_sha256"] == timeline["content_sha256"]
            and existing["model"] == llm.model):
        log.info("summary for %s is up to date, skipping the LLM", msg.video_id)
        return None

    transcript = json.loads(storage.get_bytes(timeline["timeline_uri"]))
    text = summarize(transcript, llm, cfg.summary_batch_chars)

    with db.connect() as conn:
        db.upsert_summary(conn, msg.video_id, text, llm.model, timeline["content_sha256"])
    log.info("summarised %s with %s (%d chars)", msg.video_id, llm.model, len(text))
    return SummaryReady(video_id=msg.video_id, run_id=msg.run_id, model=llm.model,
                        summary_chars=len(text))


def main():
    run_worker(
        stage=STAGE,
        group_id="vidpipe-summary",
        in_topic=Topics.TIMELINE_READY,
        in_model=TimelineReady,
        out_topic=Topics.SUMMARY_READY,
        handler=handle,
        max_attempts=1,   # a CPU LLM call can take minutes; do not repeat it blindly
    )


if __name__ == "__main__":
    main()
