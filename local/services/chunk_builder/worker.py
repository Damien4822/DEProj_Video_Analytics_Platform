"""Chunk builder: audio.done + frame.described -> chunks in Postgres -> one chunk.ready each.

Both input topics only say "something finished". The worker looks at the database to decide
whether the video is complete, so it needs no state of its own and can be restarted or run
twice safely. Only the message that completes the video builds the chunks.

    python -m services.chunk_builder.worker    (run from the local/ folder)
"""
import logging

from vidpipe import db
from vidpipe.chunking import build_chunks
from vidpipe.config import cfg
from vidpipe.kafka import run_worker
from vidpipe.schemas import (
    AudioDone,
    ChunkReady,
    FrameDescribed,
    Topics,
    chunk_version_for,
)

STAGE = "chunks"
log = logging.getLogger(__name__)


def handle(msg):
    version = chunk_version_for(cfg.chunk_window_s, cfg.chunk_overlap_s)
    with db.connect() as conn:
        db.lock_video(conn, msg.video_id)   # concurrent builders for one video take turns
        progress = db.get_progress(conn, msg.video_id)
        if not db.is_complete(progress):
            log.debug("video %s not complete yet", msg.video_id)
            return None

        audio, visual = db.load_timeline(conn, msg.video_id)
        chunks = build_chunks(
            msg.video_id, progress["duration_s"], audio, visual,
            cfg.chunk_window_s, cfg.chunk_overlap_s,
        )
        if not chunks:
            raise ValueError(f"video {msg.video_id} is complete but produced no chunks")
        if not db.replace_chunks(conn, msg.video_id, version, chunks):
            log.info("chunks for %s unchanged, nothing to emit", msg.video_id)
            return None
        db.record_stage(conn, msg.run_id, msg.video_id, STAGE, "succeeded")

    log.info("built %d chunks (%s) for %s", len(chunks), version, msg.video_id)
    return [
        ChunkReady(
            video_id=msg.video_id, run_id=msg.run_id, chunk_id=c["chunk_id"],
            chunk_idx=c["chunk_idx"], chunk_version=version, start_s=c["start_s"],
            end_s=c["end_s"], chunk_count=len(chunks),
        )
        for c in chunks
    ]


def main():
    run_worker(
        stage=STAGE,
        group_id="vidpipe-chunks",
        in_topic={Topics.AUDIO_DONE: AudioDone, Topics.FRAME_DESCRIBED: FrameDescribed},
        out_topic=Topics.CHUNK_READY,
        handler=handle,
        track_run=False,   # triggered once per frame; success is recorded when chunks are built
    )


if __name__ == "__main__":
    main()
