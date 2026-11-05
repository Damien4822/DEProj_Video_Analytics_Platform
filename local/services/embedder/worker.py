"""Embedder: chunk.ready -> embedding vector in Postgres (pgvector) -> chunk.embedded.

The message only names the chunk. The worker reads its text from the database, embeds it and
stores the vector, keyed by (chunk, model). A redelivered message does not compute the vector
again, but it still emits chunk.embedded, so a crash between the store and the publish loses nothing.

    python -m services.embedder.worker    (run from the local/ folder)
"""
import logging

from vidpipe import db
from vidpipe.kafka import run_worker
from vidpipe.models.embedder import TextEmbedder
from vidpipe.schemas import ChunkEmbedded, ChunkReady, Topics

STAGE = "embed"
log = logging.getLogger(__name__)

embedder = TextEmbedder()   # the model loads on the first chunk, once per process


def handle(msg: ChunkReady):
    with db.connect() as conn:
        chunk = db.get_chunk(conn, msg.chunk_id)
        dim = db.get_embedding_dim(conn, msg.chunk_id, embedder.model_id) if chunk else None
    if chunk is None:
        # The chunks were rebuilt with other parameters since this message was sent.
        log.warning("chunk %s of video %s no longer exists, skipping", msg.chunk_id, msg.video_id)
        return None

    if dim is None:
        vector = embedder.embed_passages([chunk["text"]])[0]
        with db.connect() as conn:
            db.upsert_embedding(conn, msg.chunk_id, embedder.model_id, vector)
        dim = len(vector)
        log.info("embedded chunk %d/%d of %s (%d dims)", msg.chunk_idx + 1, msg.chunk_count,
                 msg.video_id, dim)
    else:
        log.info("chunk %s already embedded with %s", msg.chunk_id, embedder.model_id)

    return ChunkEmbedded(
        video_id=msg.video_id, run_id=msg.run_id, chunk_id=msg.chunk_id, chunk_idx=msg.chunk_idx,
        chunk_version=msg.chunk_version, chunk_count=msg.chunk_count, model=embedder.model_id, dim=dim,
    )


def main():
    run_worker(
        stage=STAGE,
        group_id="vidpipe-embed",
        in_topic=Topics.CHUNK_READY,
        in_model=ChunkReady,
        out_topic=Topics.CHUNK_EMBEDDED,
        handler=handle,
        track_run=False,   # one message per chunk; failures are still recorded
    )


if __name__ == "__main__":
    main()
