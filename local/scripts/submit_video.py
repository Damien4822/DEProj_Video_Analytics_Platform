"""Submit a video to the pipeline by publishing to video.requested.

    python scripts/submit_video.py "https://www.ted.com/talks/..."
    python scripts/submit_video.py --file demo/video/data/raw/1183.mp4

A URL is downloaded by the downloader worker. A file is uploaded to object storage here
and the worker only registers it. The video_id is deterministic, so submitting the same
URL or file again refers to the same video and reuses what is already stored.
"""
import argparse
import hashlib
import sys
from pathlib import Path

from vidpipe import storage
from vidpipe.kafka import make_producer, publish
from vidpipe.schemas import Topics, VideoRequested, new_run_id, video_id_for_sha256, video_id_for_url


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("url", nargs="?", help="video page or file URL (anything yt-dlp supports)")
    parser.add_argument("--file", type=Path, help="local video file to upload instead of a URL")
    args = parser.parse_args()

    if bool(args.url) == bool(args.file):
        parser.error("give exactly one of a URL or --file")

    run_id = new_run_id()
    if args.file:
        if not args.file.is_file():
            sys.exit(f"not a file: {args.file}")
        video_id = video_id_for_sha256(sha256_of(args.file))
        object_uri = storage.upload_file(args.file, f"raw/{video_id}/{args.file.name}")
        message = VideoRequested(video_id=video_id, run_id=run_id, object_uri=object_uri)
    else:
        video_id = video_id_for_url(args.url)
        message = VideoRequested(video_id=video_id, run_id=run_id, source_url=args.url)

    publish(make_producer(), Topics.VIDEO_REQUESTED, message)
    print(f"submitted video_id={video_id} run_id={run_id}")


if __name__ == "__main__":
    main()
