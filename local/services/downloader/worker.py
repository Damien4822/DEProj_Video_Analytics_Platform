"""Downloader: video.requested -> object storage + videos row -> video.stored.

    python -m services.downloader.worker      (run from the local/ folder)
"""
import logging
import tempfile
from pathlib import Path

import ffmpeg
import yt_dlp

from vidpipe import db, storage
from vidpipe.config import cfg
from vidpipe.kafka import run_worker
from vidpipe.schemas import Topics, VideoRequested, VideoStored

STAGE = "download"
log = logging.getLogger(__name__)


def probe_duration(path):
    return float(ffmpeg.probe(str(path))["format"]["duration"])


def download(url, workdir):
    """Download with yt-dlp. Returns (file path, title). The container (mp4, webm, ...) is
    whatever yt-dlp picks, so callers must not assume an extension."""
    opts = {
        "format": cfg.ytdlp_format,
        "outtmpl": str(Path(workdir) / "%(id)s.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
    }
    if cfg.ytdlp_no_certifi:
        opts["compat_opts"] = {"no-certifi"}
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        path = (info.get("requested_downloads") or [{}])[0].get("filepath") or ydl.prepare_filename(info)
    return Path(path), info.get("title")


def handle(msg: VideoRequested) -> VideoStored:
    # Idempotency: a redelivered request for an already stored video does not download again.
    with db.connect() as conn:
        existing = db.get_video(conn, msg.video_id)
    if (existing and existing["storage_uri"] and existing["duration_s"] is not None
            and storage.exists(existing["storage_uri"])):
        log.info("video %s already stored, skipping download", msg.video_id)
        return VideoStored(
            video_id=msg.video_id, run_id=msg.run_id,
            video_uri=existing["storage_uri"], filename=existing["filename"],
            duration_s=existing["duration_s"], title=existing["title"],
            source_url=existing["source_url"],
        )

    with tempfile.TemporaryDirectory() as tmp:
        if msg.object_uri:
            filename = Path(storage.parse_uri(msg.object_uri)[1]).name
            local = storage.download_file(msg.object_uri, Path(tmp) / filename)
            title, video_uri = None, msg.object_uri
        else:
            local, title = download(msg.source_url, tmp)
            filename = local.name
            video_uri = storage.upload_file(local, f"raw/{msg.video_id}/{filename}")
        duration_s = probe_duration(local)

    with db.connect() as conn:
        db.upsert_video(
            conn, msg.video_id, source_url=msg.source_url, title=title,
            storage_uri=video_uri, filename=filename, duration_s=duration_s,
        )
    log.info("stored %s (%s, %.1fs)", msg.video_id, filename, duration_s)
    return VideoStored(
        video_id=msg.video_id, run_id=msg.run_id, video_uri=video_uri, filename=filename,
        duration_s=duration_s, title=title, source_url=msg.source_url,
    )


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
