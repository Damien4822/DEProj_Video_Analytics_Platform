import json

from vidpipe.timeline import build_timeline, build_transcript, content_sha256, serialize

VIDEO = {"video_id": "vid", "title": "A talk", "source_url": "http://x", "filename": "a.mp4",
         "duration_s": 60.234, "language": "en"}
AUDIO = [
    {"segment_idx": 0, "start_s": 0.0, "end_s": 10.44, "text": "first sentence"},
    {"segment_idx": 1, "start_s": 10.44, "end_s": 17.24, "text": "second sentence"},
]
VISUAL = [
    {"frame_idx": 0, "start_s": 0.0, "end_s": 12.0, "caption": "a logo",
     "objects": [{"label": "poster", "bbox": [1, 0, 10, 10]}],
     "ocr": [{"text": "TED", "score": 0.99, "bbox": [1, 1, 5, 5]}, {"text": "TALKS", "score": 0.9, "bbox": [6, 1, 9, 5]}]},
    {"frame_idx": 1, "start_s": 12.0, "end_s": 60.234, "caption": "a man on a stage", "objects": [], "ocr": []},
]


def test_timeline_attaches_overlapping_speech_to_each_visual_segment():
    doc = build_transcript(VIDEO, AUDIO, VISUAL)
    first, second = doc["timeline"]
    assert first["speech"] == "first sentence second sentence"   # the second sentence starts at 10.44 < 12
    assert second["speech"] == "second sentence"                 # and runs on into the next keyframe
    assert first["caption"] == "a logo" and first["ocr_text"] == "TED TALKS"
    assert second["ocr_text"] == ""


def test_touching_segments_do_not_overlap():
    audio = [{"start": 0.0, "end": 5.0, "text": "early"}]
    visual = [{"start": 5.0, "end": 9.0, "caption": "c", "ocr": []}]
    assert build_timeline(audio, visual)[0]["speech"] == ""


def test_document_has_the_notebook_shape_and_the_video_metadata():
    doc = build_transcript(VIDEO, AUDIO, VISUAL)
    assert set(doc) >= {"duration", "language", "audio_segments", "visual_segments", "timeline"}
    assert doc["duration"] == 60.234 and doc["language"] == "en" and doc["video"] == "a.mp4"
    assert doc["audio_segments"][1] == {"segment_id": 1, "start": 10.44, "end": 17.24, "text": "second sentence"}
    assert doc["visual_segments"][0]["objects"][0]["label"] == "poster"
    assert doc["visual_segments"][0]["ocr"][1]["text"] == "TALKS"


def test_a_video_without_speech_still_gives_a_timeline():
    doc = build_transcript(VIDEO, [], VISUAL)
    assert doc["audio_segments"] == []
    assert [t["speech"] for t in doc["timeline"]] == ["", ""]


def test_serialisation_is_stable_and_keeps_unicode():
    audio = [{"segment_idx": 0, "start_s": 0.0, "end_s": 1.0, "text": "xin chào"}]
    a = serialize(build_transcript(VIDEO, audio, VISUAL))
    b = serialize(build_transcript(VIDEO, audio, VISUAL))
    assert a == b and content_sha256(a) == content_sha256(b)
    assert "xin chào" in a.decode("utf-8")
    assert json.loads(a)["audio_segments"][0]["text"] == "xin chào"


def test_a_changed_transcript_changes_the_hash():
    changed = [dict(AUDIO[0], text="different"), AUDIO[1]]
    assert (content_sha256(serialize(build_transcript(VIDEO, AUDIO, VISUAL)))
            != content_sha256(serialize(build_transcript(VIDEO, changed, VISUAL))))
