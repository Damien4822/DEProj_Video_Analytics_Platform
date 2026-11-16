# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Embed chunks
# MAGIC Databricks version of the `embedder` worker. Turns each row of the `chunks` table (notebook 03) into one vector
# MAGIC with the same `vidpipe.models.embedder.TextEmbedder` as the local pipeline, and stores them in a
# MAGIC `chunk_embeddings` Delta table. Vectors are normalised, so cosine similarity is a dot product.
# MAGIC
# MAGIC Only the video in the `filename` widget is processed (required). Resumable and idempotent: a chunk is embedded
# MAGIC only when it has no vector from this model yet, or its text has changed since (`text_sha256`). `force` redoes the
# MAGIC video. Vectors from different models sit side by side, keyed by `(chunk_id, model)`, like the local table.
# MAGIC
# MAGIC The default model is small and runs fine on CPU. For `BAAI/bge-m3` set the `model` widget and, if you like,
# MAGIC `device` to `cuda`. The last cell prints the same numbers as the local SQL checks (model, dimension, chunks
# MAGIC versus embedded), so the two sides can be compared.
# MAGIC
# MAGIC **Environment errors:** only versions should be changed. The last cell prints the versions that worked.

# COMMAND ----------

# MAGIC %pip install sentence-transformers python-dotenv

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "vidpipe", "Catalog name")
dbutils.widgets.text("schema", "vidpipe", "Schema name")
dbutils.widgets.text("filename", "", "Video file name (required)")
dbutils.widgets.text("model", "BAAI/bge-small-en-v1.5", "Embedding model")
dbutils.widgets.dropdown("device", "cpu", ["cpu", "cuda"], "Device")
dbutils.widgets.text("chunk_version", "", "Chunk version (empty = all of the video's chunks)")
dbutils.widgets.dropdown("force", "no", ["no", "yes"], "Redo finished work")
dbutils.widgets.text("write_every", "200", "Chunks per save")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
filename = dbutils.widgets.get("filename").strip()
model_name = dbutils.widgets.get("model").strip()
device = dbutils.widgets.get("device")
chunk_version = dbutils.widgets.get("chunk_version").strip()
force = dbutils.widgets.get("force") == "yes"
write_every = int(dbutils.widgets.get("write_every"))
assert catalog, "fill in the catalog widget at the top of the notebook"
assert filename, "fill in the filename widget: the video to process (same name as in notebooks 01 to 04)"

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

import torch
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType, LongType, StringType, StructField, StructType, TimestampType

from vidpipe.models.embedder import TextEmbedder

if device == "cuda":
    assert torch.cuda.is_available(), "no GPU on this compute: attach serverless GPU or set device=cpu"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find the chunks that need a vector

# COMMAND ----------

videos = spark.table(t("videos")).where(f"filename = '{filename}'").collect()
assert videos, f"{filename!r} is not in the videos table; run notebook 01 on it first"
video_id = videos[0].video_id

assert spark.catalog.tableExists(t("chunks")), "no chunks table yet: run notebook 03 first"
chunks = spark.table(t("chunks")).where(f"video_id = '{video_id}'")
if chunk_version:
    chunks = chunks.where(f"chunk_version = '{chunk_version}'")
chunks = chunks.withColumn("text_sha256", F.sha2("text", 256))
n_chunks = chunks.count()
assert n_chunks > 0, "this video has no chunks (for that version): run notebook 03 first"

table = t("chunk_embeddings")
EMB_SCHEMA = StructType([
    StructField("chunk_id", StringType()), StructField("video_id", StringType()),
    StructField("model", StringType()), StructField("dim", LongType()),
    StructField("text_sha256", StringType()), StructField("embedding", ArrayType(FloatType())),
])

if force and spark.catalog.tableExists(table):
    spark.sql(f"DELETE FROM {table} WHERE video_id = '{video_id}' AND model = '{model_name}'")

if spark.catalog.tableExists(table):
    done = (spark.table(table).where(f"video_id = '{video_id}' AND model = '{model_name}'")
            .select("chunk_id", F.col("text_sha256").alias("embedded_sha")))
    todo = (chunks.join(done, "chunk_id", "left")
            .where("embedded_sha IS NULL OR embedded_sha <> text_sha256"))
else:
    todo = chunks
todo_rows = todo.orderBy("chunk_version", "chunk_idx").select("chunk_id", "text", "text_sha256").collect()
print(f"{video_id}: {n_chunks} chunks, {len(todo_rows)} to embed with {model_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Embed and save
# MAGIC Saved every `write_every` chunks, so a quota cut loses at most one batch. Before a batch is written, any older
# MAGIC vector of the same chunks and model is deleted, so a changed chunk never keeps a stale vector.

# COMMAND ----------

start = time.time()
dim = None
if todo_rows:
    embedder = TextEmbedder(model_id=model_name, device=device)
    for i in range(0, len(todo_rows), write_every):
        batch = todo_rows[i:i + write_every]
        vectors = embedder.embed_passages([r.text for r in batch])
        dim = len(vectors[0])
        rows = [(r.chunk_id, video_id, model_name, dim, r.text_sha256, vec) for r, vec in zip(batch, vectors)]
        if spark.catalog.tableExists(table):
            ids = ", ".join(f"'{r.chunk_id}'" for r in batch)
            spark.sql(f"DELETE FROM {table} WHERE model = '{model_name}' AND chunk_id IN ({ids})")
        (spark.createDataFrame(rows, EMB_SCHEMA).withColumn("created_at", F.current_timestamp())
         .write.mode("append").saveAsTable(table))
        print(f"  saved {min(i + write_every, len(todo_rows))}/{len(todo_rows)} chunks ({time.time() - start:.0f}s)")
    del embedder
    if device == "cuda":
        torch.cuda.empty_cache()
else:
    print("nothing to embed (use force=yes to redo)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the result
# MAGIC The same questions as the local SQL: one row per model with its dimension and count, and chunks versus embedded.

# COMMAND ----------

display(spark.sql(f"SELECT model, dim, count(*) AS vectors FROM {table} WHERE video_id = '{video_id}' GROUP BY model, dim"))

# COMMAND ----------

embedded = spark.table(table).where(f"video_id = '{video_id}' AND model = '{model_name}'").count()
print(f"chunks: {n_chunks} | embedded with {model_name}: {embedded}"
      + ("" if embedded == n_chunks else "   <-- should be equal; see the cells above"))
total_s = time.time() - start
print(f"{len(todo_rows)} chunks embedded this run in {total_s:.1f}s ({total_s / max(len(todo_rows), 1):.2f}s per chunk)")

from importlib.metadata import version

print({p: version(p) for p in ["sentence-transformers", "torch", "transformers", "python-dotenv"]})
