"""Build time-window chunks from the audio and visual timelines.

A chunk is a window of the video (for example 45 s, overlapping the next one by 10 s) holding
everything that happens in it: what is said, what the scenes look like, and what text is on
screen. The combined text is what gets embedded later. This module is pure: no I/O.
"""
from vidpipe.schemas.ids import chunk_id_for, chunk_version_for


def _overlaps(start, end, w_start, w_end):
    """Does [start, end) touch the window [w_start, w_end)? Zero-length items count if inside."""
    if end <= start:
        return w_start <= start < w_end
    return start < w_end and end > w_start


def _unique_in_order(values, key=str.casefold):
    seen, out = set(), []
    for v in values:
        k = key(v)
        if k not in seen:
            seen.add(k)
            out.append(v)
    return out


def window_starts(duration_s, window_s, overlap_s):
    """Start times of the windows covering [0, duration_s].

    A trailing window whose start lies within overlap_s of the end is skipped: the previous
    window already contains everything it would.
    """
    if window_s <= 0 or overlap_s < 0 or overlap_s >= window_s:
        raise ValueError("need window_s > 0 and 0 <= overlap_s < window_s")
    stride = window_s - overlap_s
    starts, k = [], 0
    while True:
        start = k * stride
        if start >= duration_s or (k > 0 and start + overlap_s >= duration_s):
            break
        starts.append(start)
        k += 1
    return starts


def build_chunks(video_id, duration_s, audio, visual, window_s, overlap_s):
    """Return chunk dicts for one video.

    audio:  [{start_s, end_s, text}]
    visual: [{start_s, end_s, caption, ocr: [text, ...]}]   (end_s already derived)
    Windows with no content at all are dropped, and chunk_idx stays contiguous.
    """
    version = chunk_version_for(window_s, overlap_s)
    chunks = []
    for w_start in window_starts(duration_s, window_s, overlap_s):
        w_end = min(w_start + window_s, duration_s)

        speech = " ".join(
            a["text"].strip() for a in audio
            if a["text"].strip() and _overlaps(a["start_s"], a["end_s"], w_start, w_end)
        )
        in_window = [v for v in visual
                     if v["end_s"] is not None and _overlaps(v["start_s"], v["end_s"], w_start, w_end)]
        scene = " ".join(_unique_in_order(
            [v["caption"].strip() for v in in_window if v.get("caption") and v["caption"].strip()]))
        on_screen = " | ".join(_unique_in_order(
            [t.strip() for v in in_window for t in v.get("ocr", []) if t and t.strip()]))

        parts = [f"{label}: {value}" for label, value in
                 (("Speech", speech), ("Scene", scene), ("On-screen text", on_screen)) if value]
        if not parts:
            continue

        chunks.append({
            "chunk_id": chunk_id_for(video_id, version, w_start),
            "chunk_version": version,
            "chunk_idx": len(chunks),
            "start_s": round(w_start, 3),
            "end_s": round(w_end, 3),
            "speech": speech,
            "scene": scene,
            "on_screen": on_screen,
            "text": "\n".join(parts),
        })
    return chunks
