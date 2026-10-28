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


OBJECTS = [{"label": "person", "bbox": [0, 0, 10, 20]}, {"label": "cup", "bbox": [5, 5, 8, 9]}]
OCR = [{"text": "HELLO", "score": 0.9, "bbox": [1, 1, 30, 10]}]


def frame_row_counts(conn, vid):
    return tuple(
        conn.execute(f"SELECT count(*) AS n FROM {t} WHERE video_id = %s", (vid,)).fetchone()["n"]
        for t in ("visual_segments", "detected_objects", "ocr_items")
    )


def test_replaying_a_frame_creates_no_duplicates(conn, video_id):
    for _ in range(2):
        db.replace_visual_frame(conn, video_id, 0, 0.0, "s3://b/0.jpg", "a cap", "m", OBJECTS, OCR)
    assert frame_row_counts(conn, video_id) == (1, 2, 1)


def test_reprocessing_a_frame_replaces_its_children(conn, video_id):
    db.replace_visual_frame(conn, video_id, 0, 0.0, "s3://b/0.jpg", "a cap", "m", OBJECTS, OCR)
    db.replace_visual_frame(conn, video_id, 0, 0.0, "s3://b/0.jpg", "new cap", "m", OBJECTS[:1], [])
    assert frame_row_counts(conn, video_id) == (1, 1, 0)


def test_frames_may_arrive_out_of_order_and_end_times_are_derived(conn, video_id):
    db.upsert_video(conn, video_id, duration_s=10.0, keyframe_count=3)
    for idx, start in [(2, 6.0), (0, 0.0), (1, 2.5)]:          # arrival order is scrambled
        db.replace_visual_frame(conn, video_id, idx, start, f"s3://b/{idx}.jpg", f"cap {idx}", "m", [], [])
    rows = conn.execute(
        "SELECT frame_idx, start_s, end_s FROM visual_segments_timed WHERE video_id = %s ORDER BY frame_idx",
        (video_id,),
    ).fetchall()
    assert [(r["frame_idx"], r["start_s"], r["end_s"]) for r in rows] == [
        (0, 0.0, 2.5), (1, 2.5, 6.0), (2, 6.0, 10.0)]
    progress = conn.execute("SELECT * FROM video_progress WHERE video_id = %s", (video_id,)).fetchone()
    assert progress["frames_described"] == progress["keyframe_count"] == 3


def test_database_rejects_a_degenerate_box(conn, video_id):
    bad = [{"label": "x", "bbox": [5, 5, 5, 9]}]
    with pytest.raises(psycopg.errors.CheckViolation):
        db.replace_visual_frame(conn, video_id, 0, 0.0, "s3://b/0.jpg", "c", "m", bad, [])


def make_chunk(idx, start, text="Speech: hi", end=None, version="w45-o10"):
    from vidpipe.schemas import chunk_id_for

    return {"chunk_id": chunk_id_for("x", version, start), "chunk_idx": idx, "start_s": start,
            "end_s": end or start + 45, "speech": "hi", "scene": "", "on_screen": "", "text": text}


def test_chunk_replace_reports_whether_anything_changed(conn, video_id):
    chunks = [make_chunk(0, 0.0), make_chunk(1, 35.0)]
    assert db.replace_chunks(conn, video_id, "w45-o10", chunks) is True
    assert db.replace_chunks(conn, video_id, "w45-o10", chunks) is False      # replay: no change
    n = conn.execute("SELECT count(*) AS n FROM chunks WHERE video_id = %s", (video_id,)).fetchone()["n"]
    assert n == 2


def test_chunk_replace_removes_stale_chunks_and_keeps_other_versions(conn, video_id):
    db.replace_chunks(conn, video_id, "w45-o10", [make_chunk(0, 0.0), make_chunk(1, 35.0)])
    db.replace_chunks(conn, video_id, "w30-o5", [make_chunk(0, 0.0, version="w30-o5")])
    assert db.replace_chunks(conn, video_id, "w45-o10", [make_chunk(0, 0.0, text="Speech: changed")]) is True
    rows = conn.execute("SELECT chunk_version, count(*) AS n FROM chunks WHERE video_id = %s "
                        "GROUP BY chunk_version ORDER BY chunk_version", (video_id,)).fetchall()
    assert [(r["chunk_version"], r["n"]) for r in rows] == [("w30-o5", 1), ("w45-o10", 1)]


def test_database_rejects_blank_chunk_text(conn, video_id):
    with pytest.raises(psycopg.errors.CheckViolation):
        db.replace_chunks(conn, video_id, "w45-o10", [make_chunk(0, 0.0, text="   ")])


def test_completion_follows_the_stored_data(conn, video_id):
    from services.chunk_builder.worker import is_complete

    db.upsert_video(conn, video_id, duration_s=10.0)
    assert not is_complete(db.get_progress(conn, video_id))                  # nothing finished
    db.upsert_video(conn, video_id, keyframe_count=2)
    db.replace_visual_frame(conn, video_id, 0, 0.0, "s3://b/0", "a", "m", [], [])
    db.replace_visual_frame(conn, video_id, 1, 5.0, "s3://b/1", "b", "m", [], [])
    assert not is_complete(db.get_progress(conn, video_id))                  # frames done, audio not
    db.upsert_video(conn, video_id, audio_segment_count=0)
    assert is_complete(db.get_progress(conn, video_id))                      # silent video counts as done


def test_timeline_includes_ocr_text_and_derived_end_times(conn, video_id):
    db.upsert_video(conn, video_id, duration_s=10.0)
    db.replace_visual_frame(conn, video_id, 0, 0.0, "s3://b/0", "a", "m", [],
                            [{"text": "HELLO", "score": 0.9, "bbox": [0, 0, 5, 5]},
                             {"text": "WORLD", "score": 0.8, "bbox": [6, 0, 9, 5]}])
    db.replace_visual_frame(conn, video_id, 1, 4.0, "s3://b/1", "b", "m", [], [])
    audio, visual = db.load_timeline(conn, video_id)
    assert [(r["frame_idx"], r["end_s"], r["ocr"]) for r in visual] == [
        (0, 4.0, ["HELLO", "WORLD"]), (1, 10.0, [])]


def test_transcript_data_carries_end_times_objects_and_ocr(conn, video_id):
    db.upsert_video(conn, video_id, duration_s=10.0)
    db.replace_audio_segments(conn, video_id, SEGMENTS, "small")
    db.replace_visual_frame(conn, video_id, 0, 0.0, "s3://b/0.jpg", "first", "m", OBJECTS, OCR)
    db.replace_visual_frame(conn, video_id, 1, 4.0, "s3://b/1.jpg", "second", "m", [], [])
    video, audio, visual = db.load_transcript_data(conn, video_id)
    assert video["duration_s"] == 10.0
    assert [a["text"] for a in audio] == ["hello", "world"]
    assert [(v["start_s"], v["end_s"]) for v in visual] == [(0.0, 4.0), (4.0, 10.0)]
    assert [o["label"] for o in visual[0]["objects"]] == ["person", "cup"]
    assert visual[0]["objects"][0]["bbox"] == [0, 0, 10, 20]
    assert visual[0]["ocr"][0]["text"] == "HELLO"
    assert visual[1]["objects"] == [] and visual[1]["ocr"] == []


def test_upserting_a_timeline_twice_keeps_one_row(conn, video_id):
    db.upsert_timeline(conn, video_id, "s3://b/t.json", "aa", 2, 1)
    db.upsert_timeline(conn, video_id, "s3://b/t.json", "bb", 3, 2)
    row = db.get_timeline(conn, video_id)
    assert (row["content_sha256"], row["audio_segment_count"], row["visual_segment_count"]) == ("bb", 3, 2)
    n = conn.execute("SELECT count(*) AS n FROM video_timelines WHERE video_id = %s", (video_id,)).fetchone()["n"]
    assert n == 1


def test_is_complete_needs_audio_keyframes_and_every_frame(conn, video_id):
    assert not db.is_complete(db.get_progress(conn, video_id))
    db.upsert_video(conn, video_id, duration_s=10.0, keyframe_count=2)
    db.replace_visual_frame(conn, video_id, 0, 0.0, "s3://b/0.jpg", "a", "m", [], [])
    assert not db.is_complete(db.get_progress(conn, video_id))          # no audio, one frame missing
    db.upsert_video(conn, video_id, audio_segment_count=0)
    assert not db.is_complete(db.get_progress(conn, video_id))          # one frame still missing
    db.replace_visual_frame(conn, video_id, 1, 4.0, "s3://b/1.jpg", "b", "m", [], [])
    assert db.is_complete(db.get_progress(conn, video_id))


def test_upserting_a_summary_twice_keeps_one_row_with_the_latest(conn, video_id):
    db.upsert_summary(conn, video_id, "first", "m1", "aa")
    db.upsert_summary(conn, video_id, "second", "m2", "bb")
    row = db.get_summary(conn, video_id)
    assert (row["summary"], row["model"], row["source_sha256"]) == ("second", "m2", "bb")
    n = conn.execute("SELECT count(*) AS n FROM video_summaries WHERE video_id = %s", (video_id,)).fetchone()["n"]
    assert n == 1


def test_database_rejects_an_empty_summary(conn, video_id):
    with pytest.raises(psycopg.errors.CheckViolation):
        db.upsert_summary(conn, video_id, "   ", "m", "aa")
