"""Data-quality checks on model output, applied before anything is written to the database."""


def clean_segments(raw_segments, duration_s):
    """Normalise ASR segments and report what was fixed or dropped.

    raw_segments: iterable of dicts with start, end, text (seconds, string).
    Returns (segments, issues). Segments are sorted, have a segment_idx, and satisfy
    0 <= start <= end <= duration_s, with non-empty text.
    """
    issues = []
    cleaned = []
    for i, seg in enumerate(sorted(raw_segments, key=lambda s: s["start"])):
        text = (seg.get("text") or "").strip()
        start, end = float(seg["start"]), float(seg["end"])

        if not text:
            issues.append(f"segment {i}: empty text, dropped")
            continue
        if start >= duration_s:
            issues.append(f"segment {i}: starts at {start:.2f}s, after the end of the video, dropped")
            continue
        if start < 0:
            issues.append(f"segment {i}: negative start {start:.2f}s, set to 0")
            start = 0.0
        if end > duration_s:
            issues.append(f"segment {i}: end {end:.2f}s beyond duration {duration_s:.2f}s, clamped")
            end = duration_s
        if end < start:
            issues.append(f"segment {i}: end before start, end set to start")
            end = start
        if cleaned and start < cleaned[-1]["end_s"]:
            issues.append(f"segment {i}: overlaps the previous segment")

        cleaned.append({
            "segment_idx": len(cleaned),
            "start_s": round(start, 3),
            "end_s": round(end, 3),
            "text": text,
        })
    return cleaned, issues


def _clamp_box(bbox, width, height):
    x1, y1, x2, y2 = (float(v) for v in bbox)
    return [max(0.0, min(x1, width)), max(0.0, min(y1, height)),
            max(0.0, min(x2, width)), max(0.0, min(y2, height))]


def clean_boxes(items, width, height, text_key):
    """Clamp boxes into the image and drop empty labels or degenerate boxes.

    items: dicts with text_key ('label' or 'text') and bbox [x1, y1, x2, y2].
    Returns (items, issues).
    """
    issues, cleaned = [], []
    for i, item in enumerate(items):
        text = (item.get(text_key) or "").strip()
        if not text:
            issues.append(f"item {i}: empty {text_key}, dropped")
            continue
        box = _clamp_box(item["bbox"], width, height)
        if box[2] <= box[0] or box[3] <= box[1]:
            issues.append(f"item {i}: degenerate box {item['bbox']}, dropped")
            continue
        cleaned.append({**item, text_key: text, "bbox": box})
    return cleaned, issues
