"""Ingest a video: get it into object storage, register it, and announce it to the pipeline.

This is the first stage of the pipeline (what the `downloader` worker does for a `video.requested`
message), written as plain functions so it can run from a command line (`scripts/ingest_video.py`)
or from a notebook, on the machine that has the video and the network access:

    from vidpipe.ingest import ingest
    ingest(url="https://...")          # download here, store, announce on video.stored
    ingest(file="path/to/video.mp4")   # or a file you already have

The heavy imports (yt-dlp, ffmpeg, Kafka) happen inside the functions, so importing this module is cheap.
"""
import hashlib
import logging
import tempfile
from pathlib import Path

from vidpipe import db, storage
from vidpipe.config import cfg
from vidpipe.schemas import Topics, VideoStored, new_run_id, video_id_for_sha256, video_id_for_url

STAGE = "download"
log = logging.getLogger(__name__)


def probe_duration(path):
    import ffmpeg

    return float(ffmpeg.probe(str(path))["format"]["duration"])


def download(url, workdir):
    """Download with yt-dlp. Returns (file path, title). The container (mp4, webm, ...) is
    whatever yt-dlp picks, so callers must not assume an extension."""
    import yt_dlp

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


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def store_video(video_id, run_id, *, source_url=None, object_uri=None, local_file=None):
    """Make sure the video is in object storage and registered; return the VideoStored event.

    Give exactly one of source_url (downloaded here), object_uri (already in storage) or
    local_file (uploaded here). Idempotent: a video that is already stored is not fetched again.
    """
    if sum(bool(x) for x in (source_url, object_uri, local_file)) != 1:
        raise ValueError("give exactly one of source_url, object_uri or local_file")

    with db.connect() as conn:
        existing = db.get_video(conn, video_id)
    if (existing and existing["storage_uri"] and existing["duration_s"] is not None
            and storage.exists(existing["storage_uri"])):
        log.info("video %s already stored, skipping download", video_id)
        return VideoStored(
            video_id=video_id, run_id=run_id,
            video_uri=existing["storage_uri"], filename=existing["filename"],
            duration_s=existing["duration_s"], title=existing["title"],
            source_url=existing["source_url"],
        )

    with tempfile.TemporaryDirectory() as tmp:
        title = None
        if object_uri:
            filename = Path(storage.parse_uri(object_uri)[1]).name
            local = storage.download_file(object_uri, Path(tmp) / filename)
            video_uri = object_uri
        elif local_file:
            local = Path(local_file)
            filename = local.name
            video_uri = storage.upload_file(local, f"raw/{video_id}/{filename}")
        else:
            local, title = download(source_url, tmp)
            filename = local.name
            video_uri = storage.upload_file(local, f"raw/{video_id}/{filename}")
        duration_s = probe_duration(local)

    with db.connect() as conn:
        db.upsert_video(
            conn, video_id, source_url=source_url, title=title,
            storage_uri=video_uri, filename=filename, duration_s=duration_s,
        )
    log.info("stored %s (%s, %.1fs)", video_id, filename, duration_s)
    return VideoStored(
        video_id=video_id, run_id=run_id, video_uri=video_uri, filename=filename,
        duration_s=duration_s, title=title, source_url=source_url,
    )


def ingest(url=None, file=None, announce=True):
    """Store one video (from a URL or a file) and, by default, announce it on `video.stored` so the
    audio and frame workers start. Records the stage in processing_runs. Returns the VideoStored event."""
    if bool(url) == bool(file):
        raise ValueError("give exactly one of url or file")
    run_id = new_run_id()
    if file:
        file = Path(file)
        if not file.is_file():
            raise FileNotFoundError(f"not a file: {file}")
        video_id = video_id_for_sha256(sha256_of(file))
    else:
        video_id = video_id_for_url(url)

    with db.connect() as conn:
        db.record_stage(conn, run_id, video_id, STAGE, "started")
    try:
        stored = store_video(video_id, run_id, source_url=url, local_file=file)
    except Exception as exc:
        with db.connect() as conn:
            db.record_stage(conn, run_id, video_id, STAGE, "failed", f"{type(exc).__name__}: {exc}")
        raise
    with db.connect() as conn:
        db.record_stage(conn, run_id, video_id, STAGE, "succeeded")

    if announce:
        from vidpipe.kafka import make_producer, publish

        publish(make_producer(), Topics.VIDEO_STORED, stored)
    return stored
