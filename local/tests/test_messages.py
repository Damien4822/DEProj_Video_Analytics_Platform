import pytest
from pydantic import ValidationError

from vidpipe.schemas import (
    AudioDone,
    VideoRequested,
    VideoStored,
    video_id_for_sha256,
    video_id_for_url,
)


def test_video_id_is_deterministic():
    url = "https://example.com/watch?v=abc"
    assert video_id_for_url(url) == video_id_for_url(url)
    assert video_id_for_url(url) != video_id_for_url(url + "2")


def test_video_id_ignores_whitespace_and_fragment():
    assert video_id_for_url(" https://example.com/v#t=30 ") == video_id_for_url("https://example.com/v")


def test_video_id_for_content_differs_from_url():
    assert video_id_for_sha256("ab" * 32) != video_id_for_url("ab" * 32)


def test_request_needs_exactly_one_source():
    with pytest.raises(ValidationError):
        VideoRequested(video_id="v", run_id="r")
    with pytest.raises(ValidationError):
        VideoRequested(video_id="v", run_id="r", source_url="http://x", object_uri="s3://b/k")
    assert VideoRequested(video_id="v", run_id="r", source_url="http://x").source_url == "http://x"


def test_roundtrip_through_json():
    msg = VideoStored(video_id="v", run_id="r", video_uri="s3://b/k.webm", filename="k.webm", duration_s=12.5)
    assert VideoStored.model_validate_json(msg.model_dump_json()) == msg


def test_unknown_fields_are_ignored():
    raw = '{"video_id":"v","run_id":"r","video_uri":"s3://b/k","filename":"k","duration_s":1,"new_field":1}'
    assert VideoStored.model_validate_json(raw).filename == "k"


def test_missing_required_field_is_rejected():
    with pytest.raises(ValidationError):
        AudioDone.model_validate_json('{"video_id":"v","run_id":"r"}')


def test_frame_messages_are_keyed_per_frame_but_video_messages_per_video():
    from vidpipe.schemas import FrameExtracted

    frame = FrameExtracted(video_id="v", run_id="r", frame_idx=3, start_s=1.0,
                           frame_uri="s3://b/f.jpg", frame_count=5, width=10, height=10)
    stored = VideoStored(video_id="v", run_id="r", video_uri="s3://b/k", filename="k", duration_s=1)
    assert frame.partition_key() == "v:3"
    assert stored.partition_key() == "v"


def test_chunk_ready_is_keyed_by_chunk():
    from vidpipe.schemas import ChunkReady

    msg = ChunkReady(video_id="v", run_id="r", chunk_id="c1", chunk_idx=0, chunk_version="w45-o10",
                     start_s=0, end_s=45, chunk_count=3)
    assert msg.partition_key() == "c1"
