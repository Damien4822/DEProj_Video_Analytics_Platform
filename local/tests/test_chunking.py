import pytest

from vidpipe.chunking import build_chunks, window_starts
from vidpipe.schemas import chunk_id_for, chunk_version_for


def a(start, end, text):
    return {"start_s": start, "end_s": end, "text": text}


def v(start, end, caption, ocr=()):
    return {"start_s": start, "end_s": end, "caption": caption, "ocr": list(ocr)}


def test_window_starts_cover_the_video():
    assert window_starts(100, 45, 10) == [0, 35, 70]


def test_a_tail_already_covered_by_the_overlap_is_skipped():
    assert window_starts(80, 45, 10) == [0, 35]       # 70 + 10 >= 80: nothing new in a third window


def test_a_short_video_gives_one_window():
    assert window_starts(20, 45, 10) == [0]


def test_invalid_parameters_are_rejected():
    for window, overlap in [(0, 0), (10, 10), (10, -1), (10, 20)]:
        with pytest.raises(ValueError):
            window_starts(100, window, overlap)


def test_chunk_combines_speech_scene_and_on_screen_text():
    chunks = build_chunks("vid", 40, [a(0, 5, "hello there"), a(5, 9, "second line")],
                          [v(0, 40, "a man on a stage", ["30 DAYS"])], 45, 10)
    assert len(chunks) == 1
    c = chunks[0]
    assert c["speech"] == "hello there second line"
    assert c["scene"] == "a man on a stage"
    assert c["on_screen"] == "30 DAYS"
    assert c["text"] == ("Speech: hello there second line\nScene: a man on a stage\n"
                         "On-screen text: 30 DAYS")
    assert (c["start_s"], c["end_s"]) == (0, 40)


def test_overlapping_windows_share_the_content_at_their_boundary():
    audio = [a(36, 38, "in the overlap")]
    chunks = build_chunks("vid", 100, audio, [], 45, 10)    # windows 0-45, 35-80, 70-100
    assert [c["chunk_idx"] for c in chunks] == [0, 1]       # the third window has no content
    assert "in the overlap" in chunks[0]["speech"] and "in the overlap" in chunks[1]["speech"]


def test_repeated_captions_and_ocr_are_deduplicated():
    visual = [v(0, 10, "a slide", ["Title"]), v(10, 20, "a slide", ["title", "Point"]), v(20, 30, "a man")]
    c = build_chunks("vid", 30, [], visual, 45, 10)[0]
    assert c["scene"] == "a slide a man"
    assert c["on_screen"] == "Title | Point"


def test_windows_without_any_content_are_dropped_and_indices_stay_contiguous():
    audio = [a(0, 5, "start"), a(75, 80, "end")]
    chunks = build_chunks("vid", 100, audio, [], 45, 10)    # window 35-80 holds "end"; 70-100 too
    assert [c["chunk_idx"] for c in chunks] == list(range(len(chunks)))
    assert all(c["text"].strip() for c in chunks)


def test_a_silent_video_still_chunks_from_the_visuals():
    chunks = build_chunks("vid", 30, [], [v(0, 30, "an empty room")], 45, 10)
    assert chunks[0]["speech"] == "" and chunks[0]["text"] == "Scene: an empty room"


def test_nothing_in_the_video_gives_no_chunks():
    assert build_chunks("vid", 30, [], [], 45, 10) == []


def test_chunk_ids_are_deterministic_and_depend_on_the_parameters():
    one = build_chunks("vid", 100, [a(0, 90, "x")], [], 45, 10)
    again = build_chunks("vid", 100, [a(0, 90, "x")], [], 45, 10)
    other = build_chunks("vid", 100, [a(0, 90, "x")], [], 30, 5)
    assert [c["chunk_id"] for c in one] == [c["chunk_id"] for c in again]
    assert {c["chunk_id"] for c in one}.isdisjoint(c["chunk_id"] for c in other)
    assert one[0]["chunk_id"] == chunk_id_for("vid", chunk_version_for(45, 10), 0)


def test_a_visual_segment_without_an_end_time_is_ignored():
    visual = [{"start_s": 0, "end_s": None, "caption": "unfinished", "ocr": []}]
    assert build_chunks("vid", 30, [a(0, 5, "hi")], visual, 45, 10)[0]["scene"] == ""
