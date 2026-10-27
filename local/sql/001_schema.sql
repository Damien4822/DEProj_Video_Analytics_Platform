-- Core schema. Idempotent: apply with `python scripts/apply_sql.py` or let Postgres run it on first start.
-- Vectors, chunks and visual tables are added in later files.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS videos (
    video_id     uuid PRIMARY KEY,           -- deterministic: same URL or content gives the same id
    source_url   text,
    title        text,
    storage_uri  text,                       -- s3://bucket/raw/<video_id>/<file>
    filename     text,
    duration_s   double precision CHECK (duration_s IS NULL OR duration_s >= 0),
    language     text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- One row per stage per run, so a failed stage is visible and a re-run is traceable.
CREATE TABLE IF NOT EXISTS processing_runs (
    run_id       uuid NOT NULL,
    video_id     uuid NOT NULL REFERENCES videos (video_id) ON DELETE CASCADE,
    stage        text NOT NULL,
    status       text NOT NULL CHECK (status IN ('started', 'succeeded', 'failed')),
    attempts     integer NOT NULL DEFAULT 0,
    error        text,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    PRIMARY KEY (run_id, stage)
);
CREATE INDEX IF NOT EXISTS idx_processing_runs_video ON processing_runs (video_id, started_at DESC);

CREATE TABLE IF NOT EXISTS audio_segments (
    video_id     uuid NOT NULL REFERENCES videos (video_id) ON DELETE CASCADE,
    segment_idx  integer NOT NULL,
    start_s      double precision NOT NULL,
    end_s        double precision NOT NULL,
    text         text NOT NULL,
    model        text,                       -- ASR model that produced the row
    PRIMARY KEY (video_id, segment_idx),
    CHECK (start_s >= 0 AND end_s >= start_s)
);
CREATE INDEX IF NOT EXISTS idx_audio_segments_time ON audio_segments (video_id, start_s);
