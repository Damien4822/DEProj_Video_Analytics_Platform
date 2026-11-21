# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v6"
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 Smoke test
# MAGIC What does Free Edition serverless let us do? Run the cells top to bottom and note which one fails.
# MAGIC Needs a short H.264 mp4 in the `raw` volume (upload it in the Catalog view).

# COMMAND ----------

# MAGIC %pip install opencv-python-headless==4.12.0.88 numpy

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog name")
dbutils.widgets.text("schema", "vidpipe", "Schema name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
assert catalog, "fill in the catalog widget at the top of the notebook"
raw_dir = f"/Volumes/{catalog}/{schema}/raw"
print(raw_dir)

# COMMAND ----------

# Make src/vidpipe importable: this notebook lives inside the Git folder, so look upwards for src/.
import sys
from pathlib import Path

repo_root = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / "src" / "vidpipe").is_dir())
sys.path.insert(0, str(repo_root / "src"))
print("repo root:", repo_root)

# COMMAND ----------

# Compute: how much do we have?
import os

print("cpus:", os.cpu_count())
print([line.strip() for line in open("/proc/meminfo") if line.startswith(("MemTotal", "MemAvailable"))])

# COMMAND ----------

# Keyframe extraction on the first video in the volume, using the same code as the Kafka worker.
import cv2
from vidpipe.keyframes import iter_keyframes

videos = [f for f in os.listdir(raw_dir) if f.lower().endswith((".mp4", ".mkv", ".webm", ".mov"))]
print("videos in raw:", videos)
video = os.path.join(raw_dir, videos[0])

cap = cv2.VideoCapture(video)
print("opened:", cap.isOpened(), "fps:", cap.get(cv2.CAP_PROP_FPS), "frames:", cap.get(cv2.CAP_PROP_FRAME_COUNT))
cap.release()

keyframes = [(round(t, 1), f.shape) for t, f in iter_keyframes(video, sample_fps=1.0, diff_threshold=8.0)]
print(len(keyframes), "keyframes; first:", keyframes[:5])

# COMMAND ----------

# Outbound network from the compute: which of these can we reach?
import urllib.request

for url in ["https://pypi.org", "https://github.com", "https://huggingface.co", "https://www.youtube.com"]:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=10) as r:
            print("OK  ", url, r.status)
    except Exception as exc:
        print("FAIL", url, type(exc).__name__, str(exc)[:80])
