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
                 filename=None, duration_s=None, language=None):
    """Insert or update a video. Fields passed as None keep their stored value."""
    conn.execute(
        """
        INSERT INTO videos (video_id, source_url, title, storage_uri, filename, duration_s, language)
        VALUES (%(video_id)s, %(source_url)s, %(title)s, %(storage_uri)s, %(filename)s,
                %(duration_s)s, %(language)s)
        ON CONFLICT (video_id) DO UPDATE SET
            source_url  = COALESCE(EXCLUDED.source_url,  videos.source_url),
            title       = COALESCE(EXCLUDED.title,       videos.title),
            storage_uri = COALESCE(EXCLUDED.storage_uri, videos.storage_uri),
            filename    = COALESCE(EXCLUDED.filename,    videos.filename),
            duration_s  = COALESCE(EXCLUDED.duration_s,  videos.duration_s),
            language    = COALESCE(EXCLUDED.language,    videos.language),
            updated_at  = now()
        """,
        dict(video_id=video_id, source_url=source_url, title=title, storage_uri=storage_uri,
             filename=filename, duration_s=duration_s, language=language),
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
