"""Downloader: video.requested -> object storage + videos row -> video.stored.

Optional: the same stage is available without Kafka as `python scripts/ingest_video.py <url>`, which is
what the README uses. Run this worker only if you want to submit videos with scripts/submit_video.py.

    python -m services.downloader.worker      (run from the local/ folder)
"""
import logging

from vidpipe.ingest import store_video
from vidpipe.kafka import run_worker
from vidpipe.schemas import Topics, VideoRequested, VideoStored

STAGE = "download"
log = logging.getLogger(__name__)


def handle(msg: VideoRequested) -> VideoStored:
    return store_video(msg.video_id, msg.run_id, source_url=msg.source_url, object_uri=msg.object_uri)


def main():
    run_worker(
        stage=STAGE,
        group_id="vidpipe-downloader",
        in_topic=Topics.VIDEO_REQUESTED,
        in_model=VideoRequested,
        out_topic=Topics.VIDEO_STORED,
        handler=handle,
    )


if __name__ == "__main__":
    main()
