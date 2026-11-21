# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v6"
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 Extract keyframes
# MAGIC Databricks version of the `frame_extractor` worker. Every video in the `raw` volume becomes keyframe JPEGs
# MAGIC in the `frames` volume plus rows in the `videos` and `frames` Delta tables, using the same `iter_keyframes`
# MAGIC code as the Kafka pipeline. Videos are processed one at a time (serverless memory is limited).
# MAGIC Safe to re-run: a video's old frames and rows are replaced.
# MAGIC
# MAGIC Only the video named in the `filename` widget is processed (the other files in `raw` are left alone), and the
# MAGIC notebook stops if it is empty. Upload the video first with `scripts/upload_video.py`; nothing is downloaded here.
# MAGIC The `videos` row also gets `source_url`, read from the register the upload script keeps in `raw`
# MAGIC (`_sources.json`); it is empty for a video that was uploaded by hand.

# COMMAND ----------

# MAGIC %pip install opencv-python-headless==4.12.0.88 numpy python-dotenv

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "vidpipe", "Catalog name")
dbutils.widgets.text("schema", "vidpipe", "Schema name")
dbutils.widgets.text("filename", "", "Video file name in raw (required)")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
filename = dbutils.widgets.get("filename").strip()
assert catalog, "fill in the catalog widget at the top of the notebook"
assert filename, "fill in the filename widget: the video in the raw volume to process"

SAMPLE_FPS = 1.0       # same defaults as SAMPLE_FPS / DIFF_THRESHOLD in vidpipe.config
DIFF_THRESHOLD = 8.0

raw_dir = f"/Volumes/{catalog}/{schema}/raw"
frames_dir = f"/Volumes/{catalog}/{schema}/frames"
videos_table = f"{catalog}.{schema}.videos"
frames_table = f"{catalog}.{schema}.frames"

# COMMAND ----------

# Make src/vidpipe importable (this notebook lives inside the Git folder).
import sys
from pathlib import Path

repo_root = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / "src" / "vidpipe").is_dir())
sys.path.insert(0, str(repo_root / "src"))

# COMMAND ----------

import json
import os
import shutil
import uuid

import cv2
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

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
                duration_s=duration_s, keyframe_count=len(rows), source_url=source_url_for(os.path.basename(video_path)))
    return video, rows


def replace_rows(table, video_id, rows, schema=None):
    if spark.catalog.tableExists(table):
        spark.sql(f"DELETE FROM {table} WHERE video_id = '{video_id}'")
    # mergeSchema lets the videos table gain the source_url column on its first run after this change
    spark.createDataFrame(rows, schema).write.mode("append").option("mergeSchema", "true").saveAsTable(table)


# Explicit, because source_url (and duration_s) can be None, which Spark cannot infer a type from.
VIDEO_SCHEMA = StructType([
    StructField("video_id", StringType()), StructField("filename", StringType()),
    StructField("storage_path", StringType()), StructField("duration_s", DoubleType()),
    StructField("keyframe_count", LongType()), StructField("source_url", StringType()),
])


def source_url_for(filename):
    """URL the upload script recorded for this file in raw/_sources.json, or None."""
    try:
        with open(os.path.join(raw_dir, "_sources.json")) as f:
            register = json.load(f)
    except FileNotFoundError:
        return None
    return next((e["url"] for e in register.values() if e["filename"] == filename), None)

# COMMAND ----------

names = sorted(f for f in os.listdir(raw_dir) if f.lower().endswith((".mp4", ".mkv", ".webm", ".mov")))
assert filename in names, f"{filename!r} is not in {raw_dir}: {names}"
names = [filename]
print(f"{len(names)} video(s) in {raw_dir}")
for name in names:
    video_id = video_id_for(name)
    video, rows = extract(os.path.join(raw_dir, name), video_id)
    replace_rows(videos_table, video_id, [video], VIDEO_SCHEMA)
    replace_rows(frames_table, video_id, rows)
    print(f"{name}: {len(rows)} keyframes of {video.duration_s:.1f}s -> {video_id}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT v.filename, v.source_url, v.duration_s, count(f.frame_idx) AS keyframes
    FROM {videos_table} v LEFT JOIN {frames_table} f USING (video_id)
    GROUP BY v.filename, v.source_url, v.duration_s"""))
