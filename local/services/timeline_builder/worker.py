"""Timeline builder: audio.done + frame.described -> whole-video transcript JSON -> timeline.ready.

The same triggers and completeness check as the chunk builder, in its own consumer group, so the
transcript (for reading) and the chunks (for embedding and search) are built in parallel. The
transcript has the shape the video notebook saves: audio_segments, visual_segments and a merged
timeline. It is written to object storage, and video_timelines points to it.

    python -m services.timeline_builder.worker    (run from the local/ folder)
"""
import logging

from vidpipe import db, storage
from vidpipe.kafka import run_worker
from vidpipe.schemas import AudioDone, FrameDescribed, TimelineReady, Topics
from vidpipe.timeline import build_transcript, content_sha256, serialize

STAGE = "timeline"
log = logging.getLogger(__name__)


def handle(msg):
    with db.connect() as conn:
        db.lock_video(conn, msg.video_id)   # concurrent builders for one video take turns
        if not db.is_complete(db.get_progress(conn, msg.video_id)):
            log.debug("video %s not complete yet", msg.video_id)
            return None

        video, audio, visual = db.load_transcript_data(conn, msg.video_id)
        transcript = build_transcript(video, audio, visual)
        data = serialize(transcript)
        digest = content_sha256(data)

        existing = db.get_timeline(conn, msg.video_id)
        if existing and existing["content_sha256"] == digest:
            log.info("timeline for %s unchanged, nothing to emit", msg.video_id)
            return None

        # The key is derived from the video, so a rebuild overwrites the same object.
        uri = storage.put_bytes(data, f"timeline/{msg.video_id}/transcript.json", "application/json")
        db.upsert_timeline(conn, msg.video_id, uri, digest, len(audio), len(visual))
        db.record_stage(conn, msg.run_id, msg.video_id, STAGE, "succeeded")

    log.info("built timeline for %s: %d audio / %d visual segments -> %s",
             msg.video_id, len(audio), len(visual), uri)
    return TimelineReady(
        video_id=msg.video_id, run_id=msg.run_id, timeline_uri=uri, duration_s=video["duration_s"],
        audio_segment_count=len(audio), visual_segment_count=len(visual),
    )


def main():
    run_worker(
        stage=STAGE,
        group_id="vidpipe-timeline",     # its own group: it sees every trigger the chunk builder sees
        in_topic={Topics.AUDIO_DONE: AudioDone, Topics.FRAME_DESCRIBED: FrameDescribed},
        out_topic=Topics.TIMELINE_READY,
        handler=handle,
        track_run=False,   # triggered once per frame; success is recorded when the timeline is built
    )


if __name__ == "__main__":
    main()
