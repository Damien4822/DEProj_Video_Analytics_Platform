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
    PIPELINE_ERRORS = "pipeline.errors"


def _now():
    return datetime.now(timezone.utc)


class Message(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_version: int = SCHEMA_VERSION
    video_id: str
    run_id: str
    emitted_at: datetime = Field(default_factory=_now)


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


class ErrorEvent(Message):
    """Dead-letter envelope for pipeline.errors."""

    stage: str
    source_topic: str
    error_type: str
    error: str
    attempts: int
    original: Optional[str] = None       # raw input message, truncated
    failed_at: datetime = Field(default_factory=_now)
