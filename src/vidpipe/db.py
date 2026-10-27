"""Postgres access. Every write is an upsert or a replace inside one transaction, so a
redelivered Kafka message cannot create duplicates."""
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from vidpipe.config import cfg


@contextmanager
def connect():
    """Connection that commits on success and rolls back on error."""
    with psycopg.connect(cfg.postgres_dsn, row_factory=dict_row) as conn:
        yield conn


def upsert_video(conn, video_id, *, source_url=None, title=None, storage_uri=None,
                 filename=None, duration_s=None, language=None, keyframe_count=None,
                 audio_segment_count=None):
    """Insert or update a video. Fields passed as None keep their stored value."""
    conn.execute(
        """
        INSERT INTO videos (video_id, source_url, title, storage_uri, filename, duration_s, language,
                            keyframe_count, audio_segment_count)
        VALUES (%(video_id)s, %(source_url)s, %(title)s, %(storage_uri)s, %(filename)s,
                %(duration_s)s, %(language)s, %(keyframe_count)s,
                %(audio_segment_count)s)
        ON CONFLICT (video_id) DO UPDATE SET
            source_url  = COALESCE(EXCLUDED.source_url,  videos.source_url),
            title       = COALESCE(EXCLUDED.title,       videos.title),
            storage_uri = COALESCE(EXCLUDED.storage_uri, videos.storage_uri),
            filename    = COALESCE(EXCLUDED.filename,    videos.filename),
            duration_s  = COALESCE(EXCLUDED.duration_s,  videos.duration_s),
            language    = COALESCE(EXCLUDED.language,    videos.language),
            keyframe_count = COALESCE(EXCLUDED.keyframe_count, videos.keyframe_count),
            audio_segment_count = COALESCE(EXCLUDED.audio_segment_count, videos.audio_segment_count),
            updated_at  = now()
        """,
        dict(video_id=video_id, source_url=source_url, title=title, storage_uri=storage_uri,
             filename=filename, duration_s=duration_s, language=language,
             keyframe_count=keyframe_count, audio_segment_count=audio_segment_count),
    )


def get_video(conn, video_id):
    return conn.execute("SELECT * FROM videos WHERE video_id = %s", (video_id,)).fetchone()


def record_stage(conn, run_id, video_id, stage, status, error=None):
    """Track one stage of one run: started, succeeded or failed."""
    conn.execute(
        "INSERT INTO videos (video_id) VALUES (%s) ON CONFLICT (video_id) DO NOTHING",
        (video_id,),
    )
    conn.execute(
        """
        INSERT INTO processing_runs (run_id, video_id, stage, status, attempts, error, finished_at)
        VALUES (%(run_id)s, %(video_id)s, %(stage)s, %(status)s,
                CASE WHEN %(status)s = 'started' THEN 1 ELSE 0 END, %(error)s,
                CASE WHEN %(status)s = 'started' THEN NULL ELSE now() END)
        ON CONFLICT (run_id, stage) DO UPDATE SET
            status      = EXCLUDED.status,
            attempts    = processing_runs.attempts + CASE WHEN EXCLUDED.status = 'started' THEN 1 ELSE 0 END,
            error       = EXCLUDED.error,
            finished_at = EXCLUDED.finished_at
        """,
        dict(run_id=run_id, video_id=video_id, stage=stage, status=status, error=error),
    )


def replace_audio_segments(conn, video_id, segments, model):
    """Replace all audio segments of a video in one transaction (idempotent)."""
    conn.execute("DELETE FROM audio_segments WHERE video_id = %s", (video_id,))
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO audio_segments (video_id, segment_idx, start_s, end_s, text, model)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [(video_id, s["segment_idx"], s["start_s"], s["end_s"], s["text"], model)
             for s in segments],
        )


def replace_visual_frame(conn, video_id, frame_idx, start_s, frame_uri, caption, model,
                         objects, ocr_items):
    """Replace one keyframe's description (segment, objects, OCR) in one transaction.
    The primary key is (video_id, frame_idx), so reprocessing a frame overwrites it."""
    conn.execute(
        "DELETE FROM visual_segments WHERE video_id = %s AND frame_idx = %s",
        (video_id, frame_idx),            # child rows go with it (ON DELETE CASCADE)
    )
    conn.execute(
        """
        INSERT INTO visual_segments (video_id, frame_idx, start_s, frame_uri, caption, model)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (video_id, frame_idx, start_s, frame_uri, caption, model),
    )
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO detected_objects (video_id, frame_idx, obj_idx, label, x1, y1, x2, y2)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [(video_id, frame_idx, i, o["label"], *o["bbox"]) for i, o in enumerate(objects)],
        )
        cur.executemany(
            """
            INSERT INTO ocr_items (video_id, frame_idx, item_idx, text, score, x1, y1, x2, y2)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [(video_id, frame_idx, i, o["text"], o.get("score"), *o["bbox"])
             for i, o in enumerate(ocr_items)],
        )


def lock_video(conn, video_id):
    """Serialise work on one video until the transaction ends (e.g. two chunk builders racing)."""
    conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (video_id,))


def get_progress(conn, video_id):
    """What has been stored so far for a video. Used to decide whether it is complete."""
    return conn.execute(
        """
        SELECT v.video_id, v.duration_s, v.keyframe_count, v.audio_segment_count,
               (SELECT count(*) FROM visual_segments s WHERE s.video_id = v.video_id) AS frames_described
        FROM videos v WHERE v.video_id = %s
        """,
        (video_id,),
    ).fetchone()


def load_timeline(conn, video_id):
    """Audio segments and visual segments (with derived end times and OCR text) in time order."""
    audio = conn.execute(
        "SELECT start_s, end_s, text FROM audio_segments WHERE video_id = %s ORDER BY start_s",
        (video_id,),
    ).fetchall()
    visual = conn.execute(
        """
        SELECT t.frame_idx, t.start_s, t.end_s, t.caption,
               COALESCE(array_agg(o.text ORDER BY o.item_idx) FILTER (WHERE o.text IS NOT NULL),
                        '{}') AS ocr
        FROM visual_segments_timed t
        LEFT JOIN ocr_items o ON o.video_id = t.video_id AND o.frame_idx = t.frame_idx
        WHERE t.video_id = %s
        GROUP BY t.frame_idx, t.start_s, t.end_s, t.caption
        ORDER BY t.frame_idx
        """,
        (video_id,),
    ).fetchall()
    return audio, visual


def replace_chunks(conn, video_id, chunk_version, chunks):
    """Store the chunks of one (video, chunk_version). Returns False if nothing changed.

    The comparison makes rebuilding idempotent: a second builder that sees identical output
    writes nothing and the caller emits no events, so downstream work is not repeated.
    """
    existing = {
        (str(r["chunk_id"]), r["text"], round(r["end_s"], 3))
        for r in conn.execute(
            "SELECT chunk_id, text, end_s FROM chunks WHERE video_id = %s AND chunk_version = %s",
            (video_id, chunk_version),
        ).fetchall()
    }
    wanted = {(c["chunk_id"], c["text"], round(c["end_s"], 3)) for c in chunks}
    if existing == wanted:
        return False

    conn.execute("DELETE FROM chunks WHERE video_id = %s AND chunk_version = %s",
                 (video_id, chunk_version))
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO chunks (chunk_id, video_id, chunk_version, chunk_idx, start_s, end_s,
                                speech, scene, on_screen, text)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [(c["chunk_id"], video_id, chunk_version, c["chunk_idx"], c["start_s"], c["end_s"],
              c["speech"], c["scene"], c["on_screen"], c["text"]) for c in chunks],
        )
    return True
