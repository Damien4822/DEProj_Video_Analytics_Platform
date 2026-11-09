# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 Build transcript
# MAGIC Databricks version of the `timeline_builder` worker. Merges the audio, visual, object and OCR tables of one
# MAGIC video into the whole-video transcript document (the same JSON the Kafka pipeline writes to object storage) and
# MAGIC saves it in a `video_timelines` Delta table. This is the document a summarizing LLM reads. It uses the same
# MAGIC `vidpipe.timeline.build_transcript`.
# MAGIC
# MAGIC The JSON is kept in a table column (`transcript_json`) with its SHA-256, so no extra volume is needed. If a re-run
# MAGIC produces identical JSON, nothing is written. Only the video in the `filename` widget is processed (required),
# MAGIC and the notebook stops if notebook 02 has not finished it.
# MAGIC
# MAGIC The summary step is not in this stage: the local pipeline uses an Ollama model, and Free Edition compute has no
# MAGIC such server and limited outbound access.

# COMMAND ----------

dbutils.widgets.text("catalog", "vidpipe", "Catalog name")
dbutils.widgets.text("schema", "vidpipe", "Schema name")
dbutils.widgets.text("filename", "", "Video file name (required)")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
filename = dbutils.widgets.get("filename").strip()
assert catalog, "fill in the catalog widget at the top of the notebook"
assert filename, "fill in the filename widget: the video to process (same name as in notebooks 01 and 02)"

t = lambda name: f"{catalog}.{schema}.{name}"

# COMMAND ----------

import sys
from pathlib import Path

repo_root = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / "src" / "vidpipe").is_dir())
sys.path.insert(0, str(repo_root / "src"))

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import LongType, StringType, StructField, StructType

from vidpipe.timeline import build_transcript, content_sha256, serialize

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the video's tables
# MAGIC The end of a visual segment is the start of the next keyframe (or the video's duration for the last one).

# COMMAND ----------

videos = spark.table(t("videos")).where(f"filename = '{filename}'").collect()
assert videos, f"{filename!r} is not in the videos table; run notebook 01 on it first"
v = videos[0]
video_id = v.video_id
duration_s = float(v.duration_s)

n_visual = spark.table(t("visual_segments")).where(f"video_id = '{video_id}'").count()
assert n_visual > 0 and n_visual >= v.keyframe_count, (
    f"notebook 02 has described {n_visual} of {v.keyframe_count} frames; finish it before building the transcript")

audio = [
    {"segment_idx": r.segment_idx, "start_s": r.start_s, "end_s": r.end_s, "text": r.text}
    for r in spark.sql(
        f"SELECT segment_idx, start_s, end_s, text FROM {t('audio_segments')} "
        f"WHERE video_id = '{video_id}' ORDER BY segment_idx").collect()
]
if not audio:
    print("note: no audio segments (the video may have no speech)")

objects_by_frame, ocr_by_frame = {}, {}
for r in spark.sql(f"""SELECT frame_idx, label, x1, y1, x2, y2 FROM {t('detected_objects')}
                       WHERE video_id = '{video_id}' ORDER BY frame_idx, obj_idx""").collect():
    objects_by_frame.setdefault(r.frame_idx, []).append({"label": r.label, "bbox": [r.x1, r.y1, r.x2, r.y2]})
for r in spark.sql(f"""SELECT frame_idx, text, score, x1, y1, x2, y2 FROM {t('ocr_items')}
                       WHERE video_id = '{video_id}' ORDER BY frame_idx, item_idx""").collect():
    ocr_by_frame.setdefault(r.frame_idx, []).append(
        {"text": r.text, "score": r.score, "bbox": [r.x1, r.y1, r.x2, r.y2]})

visual = [
    {"frame_idx": r.frame_idx, "start_s": r.start_s, "end_s": r.end_s, "caption": r.caption,
     "objects": objects_by_frame.get(r.frame_idx, []), "ocr": ocr_by_frame.get(r.frame_idx, [])}
    for r in spark.sql(f"""
        SELECT frame_idx, start_s, caption,
               COALESCE(LEAD(start_s) OVER (ORDER BY frame_idx), {duration_s}) AS end_s
        FROM {t('visual_segments')} WHERE video_id = '{video_id}' ORDER BY frame_idx""").collect()
]
print(f"{v.filename}: {duration_s:.0f}s, {len(audio)} audio segments, {len(visual)} visual segments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the transcript and save it

# COMMAND ----------

source_url = getattr(v, "source_url", None)   # absent on rows written before notebook 01 added the column
transcript = build_transcript(
    {"video_id": video_id, "title": None, "source_url": source_url, "filename": v.filename,
     "duration_s": duration_s, "language": None},
    audio, visual)
data = serialize(transcript)
digest = content_sha256(data)

table = t("video_timelines")
existing = None
if spark.catalog.tableExists(table):
    found = spark.table(table).where(f"video_id = '{video_id}'").select("content_sha256").collect()
    existing = found[0].content_sha256 if found else None

if existing == digest:
    print("transcript unchanged, nothing written")
else:
    SCHEMA = StructType([
        StructField("video_id", StringType()), StructField("transcript_json", StringType()),
        StructField("content_sha256", StringType()), StructField("audio_segments", LongType()),
        StructField("visual_segments", LongType()),
    ])
    row = [(video_id, data.decode("utf-8"), digest, len(audio), len(visual))]
    df = spark.createDataFrame(row, SCHEMA).withColumn("built_at", current_timestamp())
    if spark.catalog.tableExists(table):
        spark.sql(f"DELETE FROM {table} WHERE video_id = '{video_id}'")
    df.write.mode("append").saveAsTable(table)
    print(f"transcript saved: {len(data) / 1e3:.0f} KB, sha256 {digest[:12]}")

# COMMAND ----------

display(spark.sql(f"""SELECT video_id, audio_segments, visual_segments, length(transcript_json) AS json_chars,
                             content_sha256, built_at FROM {table} WHERE video_id = '{video_id}'"""))

# COMMAND ----------

# The first merged timeline entries: speech, caption and on-screen text for each keyframe span.
for entry in transcript["timeline"][:5]:
    print(f"{entry['start']:.0f}s-{entry['end']:.0f}s | speech: {entry['speech'][:100]!r} | "
          f"caption: {entry['caption']!r} | text: {entry['ocr_text'][:80]!r}")