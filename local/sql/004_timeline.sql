-- Whole-video transcript: the merged speech / scene / on-screen text timeline, stored as one JSON
-- document in object storage. This table only points to it. The hash lets a rebuild that produces
-- identical output be recognised, so no event is emitted twice.

CREATE TABLE IF NOT EXISTS video_timelines (
    video_id              uuid PRIMARY KEY REFERENCES videos (video_id) ON DELETE CASCADE,
    timeline_uri          text NOT NULL,             -- s3://bucket/timeline/<video_id>/transcript.json
    content_sha256        text NOT NULL,
    audio_segment_count   integer NOT NULL,
    visual_segment_count  integer NOT NULL,
    updated_at            timestamptz NOT NULL DEFAULT now()
);
