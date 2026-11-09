# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Transcribe and describe
# MAGIC Databricks version of the `audio_worker` and `vision_worker`. Reads the `videos` and `frames` tables written by
# MAGIC notebook 01 and writes `audio_segments`, `visual_segments`, `detected_objects` and `ocr_items` Delta tables,
# MAGIC using the same code as the Kafka pipeline (`FrameDescriber`, `clean_segments`, `clean_boxes`).
# MAGIC
# MAGIC Designed for a serverless GPU (1x A10, 24 GB); set the `device` widget to `cpu` to run without one.
# MAGIC Safe to re-run and **resumable**: finished work is skipped (audio if the video already has segments, vision for
# MAGIC frames already described), and vision rows are saved every `write_every` frames, so a quota cut or an error
# MAGIC loses at most one batch. Set `force` to `yes` to redo a video from scratch. Only the video in the `filename` widget is processed (required).
# MAGIC Timings are printed at the end for the quota notes.
# MAGIC
# MAGIC **Environment errors:** the package versions below are the repo's, plus an unpinned `faster-whisper`. If a
# MAGIC version conflict appears, fix only versions, then pin the working ones in the `%pip` line (the last cell prints them).

# COMMAND ----------

# MAGIC %pip install opencv-python-headless==4.12.0.88 numpy python-dotenv faster-whisper transformers==4.49.0 einops timm pillow rapidocr-onnxruntime

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog name")
dbutils.widgets.text("schema", "vidpipe", "Schema name")
dbutils.widgets.dropdown("device", "cuda", ["cuda", "cpu"], "Device")
dbutils.widgets.text("whisper_model", "small", "Whisper model size")
dbutils.widgets.text("vision_model", "microsoft/Florence-2-base", "Vision model")
dbutils.widgets.text("filename", "", "Video file name (required)")
dbutils.widgets.dropdown("force", "no", ["no", "yes"], "Redo finished work")
dbutils.widgets.text("write_every", "200", "Frames per save")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
device = dbutils.widgets.get("device")
whisper_model_name = dbutils.widgets.get("whisper_model")
vision_model_name = dbutils.widgets.get("vision_model")
filename = dbutils.widgets.get("filename").strip()
force = dbutils.widgets.get("force") == "yes"
write_every = int(dbutils.widgets.get("write_every"))
assert catalog, "fill in the catalog widget at the top of the notebook"
assert filename, "fill in the filename widget: the video to process (same name as in notebook 01)"

models_dir = f"/Volumes/{catalog}/{schema}/models"
t = lambda name: f"{catalog}.{schema}.{name}"

# COMMAND ----------

import os
import sys
import time
from pathlib import Path

# Model downloads are cached in the `models` volume, so a re-run does not download them again.
os.environ["HF_HOME"] = os.path.join(models_dir, "hf")

repo_root = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / "src" / "vidpipe").is_dir())
sys.path.insert(0, str(repo_root / "src"))

import cv2
import torch
from pyspark.sql import Row

from vidpipe.models.frame_describer import FrameDescriber
from vidpipe.quality import clean_boxes, clean_segments

print("device:", device, "| cuda available:", torch.cuda.is_available())
if device == "cuda":
    assert torch.cuda.is_available(), "no GPU on this compute: attach serverless GPU or set device=cpu"
    print(torch.cuda.get_device_name(0))


def replace_rows(table, video_id, rows):
    """Delete the video's old rows (if the table exists) and append the new ones."""
    if spark.catalog.tableExists(table):
        spark.sql(f"DELETE FROM {table} WHERE video_id = '{video_id}'")
    if rows:
        spark.createDataFrame(rows).write.mode("append").saveAsTable(table)


def row_count(table, video_id):
    if not spark.catalog.tableExists(table):
        return 0
    return spark.table(table).where(f"video_id = '{video_id}'").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audio: faster-whisper
# MAGIC faster-whisper decodes the audio from the video file itself, so no ffmpeg step is needed here.

# COMMAND ----------

import av
from faster_whisper import WhisperModel


def has_audio(path):
    with av.open(path) as container:
        return len(container.streams.audio) > 0


whisper = None   # loaded on first use, so a run with nothing left to transcribe skips the model load

videos = spark.table(t("videos")).where(f"filename = '{filename}'").collect()
assert videos, f"{filename!r} is not in the videos table; run notebook 01 on it first"
print(f"{len(videos)} video(s)")
audio_seconds = {}

for video in videos:
    start = time.time()
    audio_seconds[video.video_id] = 0.0
    if not has_audio(video.storage_path):
        print(f"{video.filename}: no audio stream")
        replace_rows(t("audio_segments"), video.video_id, [])
        continue
    if not force and row_count(t("audio_segments"), video.video_id) > 0:
        print(f"{video.filename}: audio already transcribed, skipped (force=yes to redo)")
        continue

    if whisper is None:
        whisper = WhisperModel(whisper_model_name, device=device,
                               compute_type="float16" if device == "cuda" else "int8")
    segments, info = whisper.transcribe(video.storage_path)
    raw = [{"start": s.start, "end": s.end, "text": s.text} for s in segments]
    cleaned, issues = clean_segments(raw, video.duration_s)
    for issue in issues:
        print("  quality:", issue)

    rows = [Row(video_id=video.video_id, segment_idx=s["segment_idx"], start_s=s["start_s"],
                end_s=s["end_s"], text=s["text"], model=whisper_model_name) for s in cleaned]
    replace_rows(t("audio_segments"), video.video_id, rows)
    audio_seconds[video.video_id] = time.time() - start
    print(f"{video.filename}: {len(rows)} segments, language {info.language}, "
          f"{audio_seconds[video.video_id]:.1f}s for {video.duration_s:.0f}s of video")

del whisper
if device == "cuda":
    torch.cuda.empty_cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visual: Florence-2 caption and objects, RapidOCR text
# MAGIC One frame at a time, exactly as the vision worker does. Batching would mean changing `FrameDescriber`.

# COMMAND ----------

describer = FrameDescriber(model_id=vision_model_name, device=device)
vision_seconds = {}


def save_batch(video_id, segment_rows, object_rows, ocr_rows):
    """Save one batch. A frame counts as done once its visual_segments row exists, so that row goes last;
    objects and text of the same frames are deleted first, so a batch interrupted half-way is redone cleanly."""
    idxs = ", ".join(str(r.frame_idx) for r in segment_rows)
    for table, rows in ((t("detected_objects"), object_rows), (t("ocr_items"), ocr_rows)):
        if spark.catalog.tableExists(table):
            spark.sql(f"DELETE FROM {table} WHERE video_id = '{video_id}' AND frame_idx IN ({idxs})")
        if rows:
            spark.createDataFrame(rows).write.mode("append").saveAsTable(table)
    spark.createDataFrame(segment_rows).write.mode("append").saveAsTable(t("visual_segments"))


for video in videos:
    start = time.time()
    if force:
        for name in ("visual_segments", "detected_objects", "ocr_items"):
            replace_rows(t(name), video.video_id, [])

    frames = spark.table(t("frames")).where(f"video_id = '{video.video_id}'").orderBy("frame_idx").collect()
    done = set()
    if spark.catalog.tableExists(t("visual_segments")):
        done = {r.frame_idx for r in spark.table(t("visual_segments"))
                .where(f"video_id = '{video.video_id}'").select("frame_idx").collect()}
    todo = [f for f in frames if f.frame_idx not in done]
    print(f"{video.filename}: {len(frames)} frames, {len(done)} already described, {len(todo)} to do")

    segment_rows, object_rows, ocr_rows = [], [], []
    n_objects = n_ocr = 0

    for n, frame in enumerate(todo, start=1):
        image = cv2.imread(frame.frame_path)
        if image is None:
            raise ValueError(f"cannot decode frame image {frame.frame_path}")
        result = describer.describe(image)
        caption = result["caption"].strip()
        objects, issues = clean_boxes(result["objects"], frame.width, frame.height, "label")
        ocr, ocr_issues = clean_boxes(result["ocr"], frame.width, frame.height, "text")
        for issue in issues + ocr_issues:
            print(f"  quality frame {frame.frame_idx}: {issue}")

        segment_rows.append(Row(video_id=video.video_id, frame_idx=frame.frame_idx, start_s=frame.start_s,
                                frame_path=frame.frame_path, caption=caption, model=vision_model_name))
        for i, o in enumerate(objects):
            object_rows.append(Row(video_id=video.video_id, frame_idx=frame.frame_idx, obj_idx=i,
                                   label=o["label"], x1=o["bbox"][0], y1=o["bbox"][1],
                                   x2=o["bbox"][2], y2=o["bbox"][3]))
        for i, o in enumerate(ocr):
            ocr_rows.append(Row(video_id=video.video_id, frame_idx=frame.frame_idx, item_idx=i,
                                text=o["text"], score=o.get("score"), x1=o["bbox"][0], y1=o["bbox"][1],
                                x2=o["bbox"][2], y2=o["bbox"][3]))

        if n % write_every == 0 or n == len(todo):
            save_batch(video.video_id, segment_rows, object_rows, ocr_rows)
            n_objects += len(object_rows)
            n_ocr += len(ocr_rows)
            segment_rows, object_rows, ocr_rows = [], [], []
            print(f"  saved {n}/{len(todo)} frames ({time.time() - start:.0f}s)")

    vision_seconds[video.video_id] = time.time() - start
    per_frame = vision_seconds[video.video_id] / max(len(todo), 1)
    print(f"{video.filename}: {len(todo)} frames this run, {n_objects} objects, {n_ocr} text items, "
          f"{vision_seconds[video.video_id]:.1f}s ({per_frame:.2f}s per frame)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the results and record the timings

# COMMAND ----------

display(spark.sql(f"""
    SELECT v.filename, v.duration_s,
           (SELECT count(*) FROM {t('audio_segments')} a WHERE a.video_id = v.video_id) AS audio_segments,
           (SELECT count(*) FROM {t('visual_segments')} s WHERE s.video_id = v.video_id) AS visual_segments,
           v.keyframe_count
    FROM {t('videos')} v"""))

# COMMAND ----------

display(spark.sql(f"SELECT start_s, caption FROM {t('visual_segments')} ORDER BY video_id, start_s LIMIT 20"))

# COMMAND ----------

# Numbers for clouds/databricks/README.md: how much compute time per minute of video.
for video in videos:
    minutes = video.duration_s / 60
    total = audio_seconds.get(video.video_id, 0) + vision_seconds.get(video.video_id, 0)
    print(f"{video.filename}: {minutes:.1f} min of video | audio {audio_seconds.get(video.video_id, 0):.0f}s | "
          f"vision {vision_seconds.get(video.video_id, 0):.0f}s | {total / minutes:.0f}s compute per minute of video")

# The versions that worked here, to pin in the %pip line at the top of this notebook.
from importlib.metadata import version

print({p: version(p) for p in ["opencv-python-headless", "numpy", "faster-whisper", "transformers", "torch",
                               "timm", "einops", "pillow", "rapidocr-onnxruntime", "python-dotenv"]})
