-- Short summary of what happens in a video, written by a local LLM from the transcript timeline.
-- source_sha256 is the hash of the transcript it was made from (video_timelines.content_sha256):
-- if neither the transcript nor the model changed, the LLM is not called again.

CREATE TABLE IF NOT EXISTS video_summaries (
    video_id        uuid PRIMARY KEY REFERENCES videos (video_id) ON DELETE CASCADE,
    summary         text NOT NULL CHECK (length(btrim(summary)) > 0),
    model           text NOT NULL,
    source_sha256   text NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now()
);
