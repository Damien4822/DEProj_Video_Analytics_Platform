"""Kafka message contracts. Messages carry storage URIs, never media payloads.

Every message shares the same envelope fields (schema_version, video_id, run_id, emitted_at).
Unknown fields are ignored so a producer on a newer schema_version does not break old consumers.
"""
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

SCHEMA_VERSION = 1


class Topics:
    VIDEO_REQUESTED = "video.requested"
    VIDEO_STORED = "video.stored"
    AUDIO_DONE = "audio.done"
    FRAME_EXTRACTED = "frame.extracted"
    FRAME_DESCRIBED = "frame.described"
    CHUNK_READY = "chunk.ready"
    CHUNK_EMBEDDED = "chunk.embedded"
    TIMELINE_READY = "timeline.ready"
    SUMMARY_READY = "summary.ready"
    PIPELINE_ERRORS = "pipeline.errors"


def _now():
    return datetime.now(timezone.utc)


class Message(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_version: int = SCHEMA_VERSION
    video_id: str
    run_id: str
    emitted_at: datetime = Field(default_factory=_now)

    def partition_key(self):
        """Kafka message key. Messages with the same key stay ordered within a partition."""
        return self.video_id


class VideoRequested(Message):
    """A video to ingest: either a URL to download or a file already in object storage."""

    source_url: Optional[str] = None
    object_uri: Optional[str] = None

    @model_validator(mode="after")
    def exactly_one_source(self):
        if bool(self.source_url) == bool(self.object_uri):
            raise ValueError("set exactly one of source_url or object_uri")
        return self


class VideoStored(Message):
    video_uri: str
    filename: str
    duration_s: float
    title: Optional[str] = None
    source_url: Optional[str] = None


class AudioDone(Message):
    audio_uri: Optional[str] = None      # None when the video has no audio stream
    duration_s: float
    segment_count: int
    asr_model: str
    language: Optional[str] = None
    language_probability: Optional[float] = None


class FrameExtracted(Message):
    """One keyframe, stored as an image. Keyed per frame so vision workers share the load."""

    frame_idx: int
    start_s: float
    frame_uri: str
    frame_count: int                     # total keyframes of this video (completion check)
    width: int
    height: int

    def partition_key(self):
        return f"{self.video_id}:{self.frame_idx}"


class FrameDescribed(Message):
    frame_idx: int
    start_s: float
    frame_count: int
    caption: str
    object_count: int
    ocr_count: int
    vision_model: str

    def partition_key(self):
        return f"{self.video_id}:{self.frame_idx}"


class ChunkReady(Message):
    """One chunk of a video is built and stored in Postgres. The text is read from there."""

    chunk_id: str
    chunk_idx: int
    chunk_version: str                   # chunking parameters, e.g. "w45-o10"
    start_s: float
    end_s: float
    chunk_count: int

    def partition_key(self):
        return self.chunk_id


class ChunkEmbedded(Message):
    """The embedding of one chunk is stored in Postgres (chunk_embeddings)."""

    chunk_id: str
    chunk_idx: int
    chunk_version: str
    chunk_count: int
    model: str                           # embedding model that produced the vector
    dim: int

    def partition_key(self):
        return self.chunk_id


class TimelineReady(Message):
    """The whole-video transcript (speech, scenes, on-screen text on one timeline) is stored as JSON."""

    timeline_uri: str
    duration_s: float
    audio_segment_count: int
    visual_segment_count: int


class SummaryReady(Message):
    """The short summary of a video is stored in Postgres (video_summaries)."""

    model: str
    summary_chars: int


class ErrorEvent(Message):
    """Dead-letter envelope for pipeline.errors."""

    stage: str
    source_topic: str
    error_type: str
    error: str
    attempts: int
    original: Optional[str] = None       # raw input message, truncated
    failed_at: datetime = Field(default_factory=_now)
