from vidpipe.quality import clean_segments


def seg(start, end, text="hello"):
    return {"start": start, "end": end, "text": text}


def test_valid_segments_pass_unchanged():
    cleaned, issues = clean_segments([seg(0, 2), seg(2, 5, "world")], duration_s=10)
    assert issues == []
    assert [s["segment_idx"] for s in cleaned] == [0, 1]
    assert cleaned[1]["text"] == "world"


def test_empty_text_is_dropped_and_indices_stay_contiguous():
    cleaned, issues = clean_segments([seg(0, 1), seg(1, 2, "  "), seg(2, 3, "x")], duration_s=10)
    assert [s["segment_idx"] for s in cleaned] == [0, 1]
    assert len(issues) == 1


def test_end_beyond_duration_is_clamped():
    cleaned, issues = clean_segments([seg(8, 12)], duration_s=10)
    assert cleaned[0]["end_s"] == 10
    assert issues


def test_segment_after_the_end_is_dropped():
    cleaned, _ = clean_segments([seg(11, 12)], duration_s=10)
    assert cleaned == []


def test_end_before_start_is_repaired():
    cleaned, _ = clean_segments([seg(5, 4)], duration_s=10)
    assert cleaned[0]["end_s"] == cleaned[0]["start_s"] == 5


def test_output_satisfies_the_database_constraint():
    cleaned, _ = clean_segments([seg(-1, 3), seg(9, 20), seg(4, 2)], duration_s=10)
    for s in cleaned:
        assert 0 <= s["start_s"] <= s["end_s"] <= 10


def test_unsorted_input_is_sorted():
    cleaned, _ = clean_segments([seg(5, 6, "b"), seg(0, 1, "a")], duration_s=10)
    assert [s["text"] for s in cleaned] == ["a", "b"]


def test_clean_boxes_clamps_and_drops():
    from vidpipe.quality import clean_boxes

    items = [
        {"label": "person", "bbox": [-5, 10, 50, 500]},   # clamped into a 100x100 image
        {"label": " ", "bbox": [0, 0, 10, 10]},            # empty label
        {"label": "cup", "bbox": [20, 20, 20, 40]},        # zero width
        {"label": "far", "bbox": [200, 200, 300, 300]},    # entirely outside: collapses to a point
    ]
    cleaned, issues = clean_boxes(items, 100, 100, "label")
    assert [c["label"] for c in cleaned] == ["person"]
    assert cleaned[0]["bbox"] == [0.0, 10.0, 50.0, 100.0]
    assert len(issues) == 3


def test_clean_boxes_keeps_other_fields():
    from vidpipe.quality import clean_boxes

    cleaned, _ = clean_boxes([{"text": " hi ", "score": 0.9, "bbox": [0, 0, 5, 5]}], 10, 10, "text")
    assert cleaned == [{"text": "hi", "score": 0.9, "bbox": [0.0, 0.0, 5.0, 5.0]}]
