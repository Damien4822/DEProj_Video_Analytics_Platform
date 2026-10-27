-- Visual side: one row per keyframe, plus the objects and on-screen text found in it.
-- Workers store only start_s. The end of a segment is derived by a view (the next keyframe's
-- start, or the video's duration for the last one), so frames can finish in any order.

ALTER TABLE videos ADD COLUMN IF NOT EXISTS keyframe_count integer;   -- set by the frame extractor

CREATE TABLE IF NOT EXISTS visual_segments (
    video_id    uuid NOT NULL REFERENCES videos (video_id) ON DELETE CASCADE,
    frame_idx   integer NOT NULL,
    start_s     double precision NOT NULL CHECK (start_s >= 0),
    frame_uri   text,
    caption     text,
    model       text,                        -- vision model that produced the row
    PRIMARY KEY (video_id, frame_idx)
);
CREATE INDEX IF NOT EXISTS idx_visual_segments_time ON visual_segments (video_id, start_s);

CREATE TABLE IF NOT EXISTS detected_objects (
    video_id    uuid NOT NULL,
    frame_idx   integer NOT NULL,
    obj_idx     integer NOT NULL,
    label       text NOT NULL,
    x1 double precision NOT NULL, y1 double precision NOT NULL,
    x2 double precision NOT NULL, y2 double precision NOT NULL,
    PRIMARY KEY (video_id, frame_idx, obj_idx),
    FOREIGN KEY (video_id, frame_idx) REFERENCES visual_segments (video_id, frame_idx) ON DELETE CASCADE,
    CHECK (x2 > x1 AND y2 > y1)
);

CREATE TABLE IF NOT EXISTS ocr_items (
    video_id    uuid NOT NULL,
    frame_idx   integer NOT NULL,
    item_idx    integer NOT NULL,
    text        text NOT NULL,
    score       double precision,
    x1 double precision NOT NULL, y1 double precision NOT NULL,
    x2 double precision NOT NULL, y2 double precision NOT NULL,
    PRIMARY KEY (video_id, frame_idx, item_idx),
    FOREIGN KEY (video_id, frame_idx) REFERENCES visual_segments (video_id, frame_idx) ON DELETE CASCADE,
    CHECK (x2 > x1 AND y2 > y1)
);

-- Visual segments with their end time derived from the next keyframe.
CREATE OR REPLACE VIEW visual_segments_timed AS
SELECT s.video_id,
       s.frame_idx,
       s.start_s,
       COALESCE(LEAD(s.start_s) OVER (PARTITION BY s.video_id ORDER BY s.frame_idx), v.duration_s) AS end_s,
       s.caption,
       s.frame_uri,
       s.model
FROM visual_segments s
JOIN videos v USING (video_id);
