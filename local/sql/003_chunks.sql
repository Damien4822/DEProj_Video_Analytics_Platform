-- Chunks: time windows of a video combining speech, scene captions and on-screen text.
-- The combined text is what gets embedded. Vectors live in their own table (added later).

ALTER TABLE videos ADD COLUMN IF NOT EXISTS audio_segment_count integer;   -- set when audio is finished

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id       uuid PRIMARY KEY,          -- deterministic from (video, chunk_version, start)
    video_id       uuid NOT NULL REFERENCES videos (video_id) ON DELETE CASCADE,
    chunk_version  text NOT NULL,             -- chunking parameters, e.g. w45-o10
    chunk_idx      integer NOT NULL,
    start_s        double precision NOT NULL,
    end_s          double precision NOT NULL,
    speech         text NOT NULL DEFAULT '',
    scene          text NOT NULL DEFAULT '',
    on_screen      text NOT NULL DEFAULT '',
    text           text NOT NULL CHECK (length(btrim(text)) > 0),
    created_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (video_id, chunk_version, chunk_idx),
    CHECK (start_s >= 0 AND end_s > start_s)
);
CREATE INDEX IF NOT EXISTS idx_chunks_video_time ON chunks (video_id, start_s);

-- Is a video fully processed? Compare what was expected with what has arrived.
-- Dropped and re-created so this file can be applied repeatedly, and on a database that has an
-- older version of the view.
DROP VIEW IF EXISTS video_progress;
CREATE VIEW video_progress AS
SELECT v.video_id,
       v.title,
       v.duration_s,
       v.keyframe_count,
       (SELECT count(*) FROM visual_segments s WHERE s.video_id = v.video_id) AS frames_described,
       (SELECT count(*) FROM audio_segments a WHERE a.video_id = v.video_id)  AS audio_segments,
       (v.audio_segment_count IS NOT NULL)                                    AS audio_done,
       (SELECT count(*) FROM chunks c WHERE c.video_id = v.video_id)          AS chunks
FROM videos v;
