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
