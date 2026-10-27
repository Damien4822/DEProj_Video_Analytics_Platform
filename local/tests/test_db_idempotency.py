"""Needs the local Postgres (docker compose up) with the schema applied; skipped otherwise."""
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from vidpipe import db  # noqa: E402


@pytest.fixture
def conn():
    try:
        with db.connect() as c:
            c.execute("SELECT 1 FROM audio_segments LIMIT 1")
    except psycopg.Error as e:
        pytest.skip(f"postgres with schema not available: {e}")
    with db.connect() as c:
        yield c


@pytest.fixture
def video_id(conn):
    vid = str(uuid.uuid4())
    db.upsert_video(conn, vid, source_url="http://example.com/test")
    conn.commit()
    yield vid
    conn.rollback()  # a test may have left the transaction aborted
    conn.execute("DELETE FROM videos WHERE video_id = %s", (vid,))
    conn.commit()


SEGMENTS = [
    {"segment_idx": 0, "start_s": 0.0, "end_s": 1.5, "text": "hello"},
    {"segment_idx": 1, "start_s": 1.5, "end_s": 3.0, "text": "world"},
]


def count(conn, vid):
    return conn.execute("SELECT count(*) AS n FROM audio_segments WHERE video_id = %s", (vid,)).fetchone()["n"]


def test_replaying_the_same_segments_creates_no_duplicates(conn, video_id):
    db.replace_audio_segments(conn, video_id, SEGMENTS, "small")
    db.replace_audio_segments(conn, video_id, SEGMENTS, "small")
    assert count(conn, video_id) == 2


def test_rerun_with_fewer_segments_leaves_no_stale_rows(conn, video_id):
    db.replace_audio_segments(conn, video_id, SEGMENTS, "small")
    db.replace_audio_segments(conn, video_id, SEGMENTS[:1], "small")
    assert count(conn, video_id) == 1


def test_upsert_video_keeps_existing_values(conn, video_id):
    db.upsert_video(conn, video_id, title="A title")
    db.upsert_video(conn, video_id, duration_s=12.0)
    row = db.get_video(conn, video_id)
    assert row["title"] == "A title" and row["duration_s"] == 12.0
    assert row["source_url"] == "http://example.com/test"


def test_stage_attempts_are_counted_and_final_status_wins(conn, video_id):
    run = str(uuid.uuid4())
    db.record_stage(conn, run, video_id, "audio", "started")
    db.record_stage(conn, run, video_id, "audio", "started")
    db.record_stage(conn, run, video_id, "audio", "failed", "boom")
    row = conn.execute("SELECT * FROM processing_runs WHERE run_id = %s", (run,)).fetchone()
    assert (row["status"], row["attempts"], row["error"]) == ("failed", 2, "boom")


def test_database_rejects_inverted_time_range(conn, video_id):
    bad = [{"segment_idx": 0, "start_s": 5.0, "end_s": 1.0, "text": "x"}]
    with pytest.raises(psycopg.errors.CheckViolation):
        db.replace_audio_segments(conn, video_id, bad, "small")
