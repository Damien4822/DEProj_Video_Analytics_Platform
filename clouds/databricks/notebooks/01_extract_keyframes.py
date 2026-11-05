# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Extract keyframes
# MAGIC Databricks version of the `frame_extractor` worker. Every video in the `raw` volume becomes keyframe JPEGs
# MAGIC in the `frames` volume plus rows in the `videos` and `frames` Delta tables, using the same `iter_keyframes`
# MAGIC code as the Kafka pipeline. Videos are processed one at a time (the compute has ~6 GB free).
# MAGIC Safe to re-run: a video's old frames and rows are replaced.
# MAGIC
# MAGIC Optional first step: put a video URL in the `video_url` widget and it is downloaded into `raw` (the
# MAGIC `downloader` worker's job locally). Leave it empty to use the videos already in `raw`.

# COMMAND ----------

# MAGIC %pip install opencv-python-headless==4.12.0.88 numpy yt-dlp[default] imageio-ffmpeg python-dotenv

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog name")
dbutils.widgets.text("schema", "vidpipe", "Schema name")
dbutils.widgets.text("video_url", "", "Video URL to download into raw (optional)")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
video_url = dbutils.widgets.get("video_url").strip()
assert catalog, "fill in the catalog widget at the top of the notebook"

SAMPLE_FPS = 1.0       # same defaults as SAMPLE_FPS / DIFF_THRESHOLD in vidpipe.config
DIFF_THRESHOLD = 8.0

raw_dir = f"/Volumes/{catalog}/{schema}/raw"
frames_dir = f"/Volumes/{catalog}/{schema}/frames"
videos_table = f"{catalog}.{schema}.videos"
frames_table = f"{catalog}.{schema}.frames"

# COMMAND ----------

# Download a video into the raw volume (skipped when the widget is empty).
# yt-dlp needs ffmpeg to merge the video and audio streams; imageio-ffmpeg ships a binary for it.
import sys
from pathlib import Path

repo_root = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / "src" / "vidpipe").is_dir())
sys.path.insert(0, str(repo_root / "src"))

if video_url:
    import imageio_ffmpeg
    import yt_dlp
    from vidpipe.config import cfg

    options = {
        "format": cfg.ytdlp_format,   # same selector as the local downloader (no AV1, up to 720p)
        "outtmpl": f"{raw_dir}/%(id)s.%(ext)s",
        "ffmpeg_location": imageio_ffmpeg.get_ffmpeg_exe(),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
    }
    with yt_dlp.YoutubeDL(options) as ydl:
        info = ydl.extract_info(video_url, download=True)
    print("downloaded:", info.get("title"), "->", ydl.prepare_filename(info))
else:
    print("video_url is empty: using the videos already in", raw_dir)

# COMMAND ----------

import os
import shutil
import uuid

import cv2
from pyspark.sql import Row

from vidpipe.keyframes import iter_keyframes  # noqa: E402


def video_id_for(filename):
    # Deterministic, so re-running the same file overwrites the same rows and images.
    return str(uuid.uuid5(uuid.NAMESPACE_URL, filename))


def extract(video_path, video_id):
    """Write keyframe JPEGs for one video; return (video row, frame rows)."""
    out_dir = os.path.join(frames_dir, video_id)
    shutil.rmtree(out_dir, ignore_errors=True)
    os.makedirs(out_dir)

    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    duration_s = cap.get(cv2.CAP_PROP_FRAME_COUNT) / fps if fps else None
    cap.release()

    rows = []
    for idx, (start_s, frame) in enumerate(iter_keyframes(video_path, SAMPLE_FPS, DIFF_THRESHOLD)):
        ok, jpg = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
        assert ok, f"could not encode frame {idx}"
        path = os.path.join(out_dir, f"{idx:06d}.jpg")
        with open(path, "wb") as f:
            f.write(jpg.tobytes())
        height, width = frame.shape[:2]
        rows.append(Row(video_id=video_id, frame_idx=idx, start_s=float(start_s),
                        frame_path=path, width=width, height=height))
    if not rows:
        raise ValueError(f"no frames could be decoded from {video_path}")
    video = Row(video_id=video_id, filename=os.path.basename(video_path), storage_path=video_path,
                duration_s=duration_s, keyframe_count=len(rows))
    return video, rows


def replace_rows(table, video_id, rows):
    if spark.catalog.tableExists(table):
        spark.sql(f"DELETE FROM {table} WHERE video_id = '{video_id}'")
    spark.createDataFrame(rows).write.mode("append").saveAsTable(table)

# COMMAND ----------

names = sorted(f for f in os.listdir(raw_dir) if f.lower().endswith((".mp4", ".mkv", ".webm", ".mov")))
print(f"{len(names)} video(s) in {raw_dir}")
for name in names:
    video_id = video_id_for(name)
    video, rows = extract(os.path.join(raw_dir, name), video_id)
    replace_rows(videos_table, video_id, [video])
    replace_rows(frames_table, video_id, rows)
    print(f"{name}: {len(rows)} keyframes of {video.duration_s:.1f}s -> {video_id}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT v.filename, v.duration_s, count(f.frame_idx) AS keyframes
    FROM {videos_table} v LEFT JOIN {frames_table} f USING (video_id)
    GROUP BY v.filename, v.duration_s"""))
