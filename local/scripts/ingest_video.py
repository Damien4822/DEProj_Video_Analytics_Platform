"""Download (or take) a video, store it, and start the pipeline. Replaces the downloader worker.

    python scripts/ingest_video.py "https://www.ted.com/talks/..."
    python scripts/ingest_video.py --file demo/video/data/raw/1183.mp4

Runs on your machine, so it uses your network, your yt-dlp and your JavaScript runtime (Deno or Node,
needed for YouTube). The video goes to object storage, its row is written to Postgres, and a
`video.stored` event tells the audio and frame workers to start. The same function is available to a
notebook: `from vidpipe.ingest import ingest`.

A video that is already stored is not downloaded again, but it is announced again, which restarts
the later stages for it (use --no-announce to only store it). Needs ffmpeg/ffprobe on the PATH.
"""
import argparse
import sys
from pathlib import Path

from vidpipe.ingest import ingest


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("url", nargs="?", help="video page or file URL (anything yt-dlp supports)")
    parser.add_argument("--file", type=Path, help="local video file to store instead of a URL")
    parser.add_argument("--no-announce", action="store_true",
                        help="store and register only; do not publish video.stored")
    args = parser.parse_args()

    if bool(args.url) == bool(args.file):
        parser.error("give exactly one of a URL or --file")
    try:
        stored = ingest(url=args.url, file=args.file, announce=not args.no_announce)
    except FileNotFoundError as exc:
        sys.exit(str(exc))
    state = "stored" if args.no_announce else "stored and announced"
    print(f"{state}: video_id={stored.video_id} file={stored.filename} duration={stored.duration_s:.0f}s")


if __name__ == "__main__":
    main()
