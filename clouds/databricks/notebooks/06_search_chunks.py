# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Search chunks
# MAGIC Databricks version of `scripts/search.py`. Embeds a question with the same model as the chunks and returns the
# MAGIC nearest chunks, with timestamps, from the `chunk_embeddings` table written by notebook 05. Normalised vectors,
# MAGIC so the score is cosine similarity. There is no vector index: the vectors are compared in memory, which is fine
# MAGIC for a few thousand chunks.
# MAGIC
# MAGIC Read-only. Leave `filename` empty to search every embedded video, or name one video. The `model` widget must
# MAGIC be a model the chunks were embedded with (notebook 05).

# COMMAND ----------

# MAGIC %pip install sentence-transformers python-dotenv

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "vidpipe", "Catalog name")
dbutils.widgets.text("schema", "vidpipe", "Schema name")
dbutils.widgets.text("query", "", "Question")
dbutils.widgets.text("filename", "", "Only this video file (empty = all embedded videos)")
dbutils.widgets.text("model", "BAAI/bge-small-en-v1.5", "Embedding model")
dbutils.widgets.dropdown("device", "cpu", ["cpu", "cuda"], "Device")
dbutils.widgets.text("limit", "5", "Number of results")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
query = dbutils.widgets.get("query").strip()
filename = dbutils.widgets.get("filename").strip()
model_name = dbutils.widgets.get("model").strip()
device = dbutils.widgets.get("device")
limit = int(dbutils.widgets.get("limit"))
assert catalog, "fill in the catalog widget at the top of the notebook"
assert query, "type a question in the query widget"

models_dir = f"/Volumes/{catalog}/{schema}/models"
t = lambda name: f"{catalog}.{schema}.{name}"

# COMMAND ----------

import os
import sys
from pathlib import Path

os.environ["HF_HOME"] = os.path.join(models_dir, "hf")

repo_root = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / "src" / "vidpipe").is_dir())
sys.path.insert(0, str(repo_root / "src"))

import numpy as np

from vidpipe.models.embedder import TextEmbedder

# COMMAND ----------

# Load the vectors of this model (optionally one video), with each chunk's text and time span.
condition = f"e.model = '{model_name}'"
if filename:
    videos = spark.table(t("videos")).where(f"filename = '{filename}'").collect()
    assert videos, f"{filename!r} is not in the videos table"
    condition += f" AND e.video_id = '{videos[0].video_id}'"

rows = spark.sql(f"""
    SELECT e.chunk_id, e.video_id, v.filename, c.chunk_idx, c.start_s, c.end_s, c.text, e.embedding
    FROM {t('chunk_embeddings')} e
    JOIN {t('chunks')} c ON c.chunk_id = e.chunk_id
    JOIN {t('videos')} v ON v.video_id = e.video_id
    WHERE {condition}""").collect()
assert rows, f"no vectors for model {model_name!r}" + (f" and video {filename!r}" if filename else "") \
    + ": run notebook 05 first (and check the model widget)"
print(f"{len(rows)} embedded chunks to search")

# COMMAND ----------

embedder = TextEmbedder(model_id=model_name, device=device)
q = np.array(embedder.embed_query(query), dtype="float32")
matrix = np.array([r.embedding for r in rows], dtype="float32")
assert matrix.shape[1] == q.shape[0], (
    f"the stored vectors have {matrix.shape[1]} dimensions but this model gives {q.shape[0]}: wrong model widget?")

scores = matrix @ q                       # cosine similarity: both sides are normalised
best = np.argsort(-scores)[:limit]


def fmt_time(seconds):
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


print(f"query: {query!r}  ({model_name})\n")
for rank, i in enumerate(best, start=1):
    r = rows[i]
    print(f"{rank}. score {scores[i]:.3f} | {r.filename} | {fmt_time(r.start_s)} to {fmt_time(r.end_s)} (chunk {r.chunk_idx})")
    print("   " + r.text.replace("\n", "\n   ")[:600] + "\n")
