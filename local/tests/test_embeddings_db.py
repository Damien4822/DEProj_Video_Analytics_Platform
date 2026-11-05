"""Needs the local Postgres (docker compose up) with 006_embeddings.sql applied; skipped otherwise."""
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from vidpipe import db  # noqa: E402

MODEL = "test-model"


@pytest.fixture
def conn():
    try:
        with db.connect() as c:
            c.execute("SELECT 1 FROM chunk_embeddings LIMIT 1")
    except psycopg.Error as e:
        pytest.skip(f"postgres with the embeddings table not available: {e}")
    with db.connect() as c:
        yield c


@pytest.fixture
def chunks(conn):
    """Three chunks of a throwaway video, with 3-dimensional vectors for a fake model."""
    video_id = str(uuid.uuid4())
    db.upsert_video(conn, video_id, source_url="http://example.com/embeddings-test", title="t")
    ids = [str(uuid.uuid4()) for _ in range(3)]
    for i, chunk_id in enumerate(ids):
        conn.execute(
            "INSERT INTO chunks (chunk_id, video_id, chunk_version, chunk_idx, start_s, end_s, text) "
            "VALUES (%s, %s, 'test', %s, %s, %s, %s)",
            (chunk_id, video_id, i, i * 10.0, i * 10.0 + 10, f"chunk {i}"),
        )
    conn.commit()
    yield video_id, ids
    conn.rollback()
    conn.execute("DELETE FROM videos WHERE video_id = %s", (video_id,))   # cascades to the rest
    conn.commit()


def test_upsert_is_idempotent_and_overwrites(conn, chunks):
    _, ids = chunks
    db.upsert_embedding(conn, ids[0], MODEL, [1.0, 0.0, 0.0])
    db.upsert_embedding(conn, ids[0], MODEL, [0.0, 1.0, 0.0])
    rows = conn.execute("SELECT dim FROM chunk_embeddings WHERE chunk_id = %s", (ids[0],)).fetchall()
    assert [r["dim"] for r in rows] == [3]
    assert db.get_embedding_dim(conn, ids[0], MODEL) == 3
    assert db.get_embedding_dim(conn, ids[0], "other-model") is None


def test_search_ranks_by_cosine_similarity(conn, chunks):
    video_id, ids = chunks
    db.upsert_embedding(conn, ids[0], MODEL, [1.0, 0.0, 0.0])
    db.upsert_embedding(conn, ids[1], MODEL, [0.0, 1.0, 0.0])
    db.upsert_embedding(conn, ids[2], MODEL, [0.9, 0.1, 0.0])

    hits = db.search_chunks(conn, [1.0, 0.0, 0.0], MODEL, limit=3, video_id=video_id)
    assert [h["text"] for h in hits] == ["chunk 0", "chunk 2", "chunk 1"]
    assert hits[0]["score"] == pytest.approx(1.0)
    assert hits[2]["score"] == pytest.approx(0.0)


def test_search_only_sees_the_requested_model_and_size(conn, chunks):
    video_id, ids = chunks
    db.upsert_embedding(conn, ids[0], MODEL, [1.0, 0.0, 0.0])
    db.upsert_embedding(conn, ids[1], "four-dim-model", [1.0, 0.0, 0.0, 0.0])
    hits = db.search_chunks(conn, [1.0, 0.0, 0.0], MODEL, video_id=video_id)
    assert [h["text"] for h in hits] == ["chunk 0"]


def test_rebuilding_a_chunk_removes_its_embedding(conn, chunks):
    _, ids = chunks
    db.upsert_embedding(conn, ids[0], MODEL, [1.0, 0.0, 0.0])
    conn.execute("DELETE FROM chunks WHERE chunk_id = %s", (ids[0],))
    assert db.get_embedding_dim(conn, ids[0], MODEL) is None
