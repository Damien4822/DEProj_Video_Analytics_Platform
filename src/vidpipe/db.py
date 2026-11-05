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


def is_complete(progress):
    """Audio finished, keyframes extracted, and every keyframe described.

    progress is a get_progress() row. Both builders (chunks, timeline) use this check."""
    return bool(
        progress
        and progress["duration_s"] is not None
        and progress["audio_segment_count"] is not None
        and progress["keyframe_count"] is not None
        and progress["frames_described"] >= progress["keyframe_count"]
    )


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


def vector_literal(values):
    """A pgvector text literal such as '[0.1,0.2]', cast with ::vector in the SQL (no driver adapter needed)."""
    return "[" + ",".join(format(float(v), ".8g") for v in values) + "]"


def get_chunk(conn, chunk_id):
    return conn.execute(
        "SELECT chunk_id, video_id, chunk_idx, text FROM chunks WHERE chunk_id = %s", (chunk_id,),
    ).fetchone()


def get_embedding_dim(conn, chunk_id, model):
    """Size of the stored vector, or None when the chunk has no embedding from this model."""
    row = conn.execute(
        "SELECT dim FROM chunk_embeddings WHERE chunk_id = %s AND model = %s", (chunk_id, model),
    ).fetchone()
    return row["dim"] if row else None


def upsert_embedding(conn, chunk_id, model, embedding):
    """Store one chunk's vector for one model. Re-embedding the same chunk overwrites it."""
    conn.execute(
        """
        INSERT INTO chunk_embeddings (chunk_id, model, dim, embedding)
        VALUES (%s, %s, %s, %s::vector)
        ON CONFLICT (chunk_id, model) DO UPDATE SET
            dim        = EXCLUDED.dim,
            embedding  = EXCLUDED.embedding,
            created_at = now()
        """,
        (chunk_id, model, len(embedding), vector_literal(embedding)),
    )


def search_chunks(conn, query_embedding, model, *, limit=5, video_id=None):
    """Nearest chunks by cosine similarity, best first, each with a score in [-1, 1].

    The query vector must come from the same model. The cast and the model filter match the
    HNSW index in 006_embeddings.sql, so the index is used when it exists.
    """
    dim = int(len(query_embedding))   # a cast needs a literal size, not a bind parameter
    params = {"q": vector_literal(query_embedding), "model": model, "limit": limit}
    video_filter = ""
    if video_id:
        video_filter = "AND c.video_id = %(video)s"
        params["video"] = video_id
    return conn.execute(
        f"""
        SELECT c.video_id, v.title, c.chunk_idx, c.start_s, c.end_s, c.text,
               1 - (e.embedding::vector({dim}) <=> %(q)s::vector({dim})) AS score
        FROM chunk_embeddings e
        JOIN chunks c ON c.chunk_id = e.chunk_id
        JOIN videos v ON v.video_id = c.video_id
        WHERE e.model = %(model)s AND e.dim = {dim} {video_filter}
        ORDER BY e.embedding::vector({dim}) <=> %(q)s::vector({dim})
        LIMIT %(limit)s
        """,
        params,
    ).fetchall()


def load_transcript_data(conn, video_id):
    """Everything the transcript document needs: (video row, audio segments, visual segments).

    Visual segments carry their derived end time, their objects and their OCR items."""
    video = get_video(conn, video_id)
    audio = conn.execute(
        "SELECT segment_idx, start_s, end_s, text FROM audio_segments "
        "WHERE video_id = %s ORDER BY segment_idx",
        (video_id,),
    ).fetchall()
    visual = conn.execute(
        """
        SELECT t.frame_idx, t.start_s, t.end_s, t.caption,
               COALESCE((SELECT json_agg(json_build_object(
                             'label', o.label, 'bbox', json_build_array(o.x1, o.y1, o.x2, o.y2))
                             ORDER BY o.obj_idx)
                         FROM detected_objects o
                         WHERE o.video_id = t.video_id AND o.frame_idx = t.frame_idx),
                        '[]'::json) AS objects,
               COALESCE((SELECT json_agg(json_build_object(
                             'text', r.text, 'score', r.score,
                             'bbox', json_build_array(r.x1, r.y1, r.x2, r.y2))
                             ORDER BY r.item_idx)
                         FROM ocr_items r
                         WHERE r.video_id = t.video_id AND r.frame_idx = t.frame_idx),
                        '[]'::json) AS ocr
        FROM visual_segments_timed t
        WHERE t.video_id = %s
        ORDER BY t.frame_idx
        """,
        (video_id,),
    ).fetchall()
    return video, audio, visual


def get_summary(conn, video_id):
    return conn.execute("SELECT * FROM video_summaries WHERE video_id = %s", (video_id,)).fetchone()


def upsert_summary(conn, video_id, summary, model, source_sha256):
    conn.execute(
        """
        INSERT INTO video_summaries (video_id, summary, model, source_sha256)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (video_id) DO UPDATE SET
            summary        = EXCLUDED.summary,
            model          = EXCLUDED.model,
            source_sha256  = EXCLUDED.source_sha256,
            updated_at     = now()
        """,
        (video_id, summary, model, source_sha256),
    )


def get_timeline(conn, video_id):
    return conn.execute("SELECT * FROM video_timelines WHERE video_id = %s", (video_id,)).fetchone()


def upsert_timeline(conn, video_id, timeline_uri, content_sha256, audio_count, visual_count):
    conn.execute(
        """
        INSERT INTO video_timelines (video_id, timeline_uri, content_sha256,
                                     audio_segment_count, visual_segment_count)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (video_id) DO UPDATE SET
            timeline_uri         = EXCLUDED.timeline_uri,
            content_sha256       = EXCLUDED.content_sha256,
            audio_segment_count  = EXCLUDED.audio_segment_count,
            visual_segment_count = EXCLUDED.visual_segment_count,
            updated_at           = now()
        """,
        (video_id, timeline_uri, content_sha256, audio_count, visual_count),
    )
