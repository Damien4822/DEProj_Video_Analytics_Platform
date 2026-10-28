"""Build the whole-video transcript document: what is said, shown and written, on one timeline.

This is the pipeline version of the JSON the video notebook saves. The `timeline` list is built
the same way: for each visual segment, the speech that overlaps it. This module is pure: no I/O.
"""
import hashlib
import json

from vidpipe.chunking import overlaps


def _t(seconds):
    return round(float(seconds), 3)


def build_timeline(audio, visual):
    """One entry per visual segment: the speech overlapping it, its caption and its on-screen text.

    A sentence that spans several keyframes appears in each of their entries, as in the notebook.
    audio:  [{start, end, text}]
    visual: [{start, end, caption, ocr: [{text, ...}]}]
    """
    return [
        {
            "start": v["start"],
            "end": v["end"],
            "speech": " ".join(
                a["text"] for a in audio if overlaps(a["start"], a["end"], v["start"], v["end"])
            ),
            "caption": v["caption"],
            "ocr_text": " ".join(o["text"] for o in v["ocr"]),
        }
        for v in visual
    ]


def build_transcript(video, audio, visual):
    """The transcript document of one video, ready to be written as JSON.

    video:  {video_id, title, source_url, filename, duration_s, language}
    audio:  [{segment_idx, start_s, end_s, text}] in time order
    visual: [{frame_idx, start_s, end_s, caption, objects: [{label, bbox}],
              ocr: [{text, score, bbox}]}] in time order, end_s already derived
    """
    audio_segments = [
        {"segment_id": a["segment_idx"], "start": _t(a["start_s"]), "end": _t(a["end_s"]),
         "text": a["text"]}
        for a in audio
    ]
    visual_segments = [
        {"segment_id": v["frame_idx"], "start": _t(v["start_s"]), "end": _t(v["end_s"]),
         "caption": v["caption"] or "", "objects": v["objects"], "ocr": v["ocr"]}
        for v in visual
    ]
    return {
        "video_id": str(video["video_id"]),
        "title": video.get("title"),
        "source_url": video.get("source_url"),
        "video": video.get("filename"),
        "duration": _t(video["duration_s"]),
        "language": video.get("language"),
        "audio_segments": audio_segments,
        "visual_segments": visual_segments,
        "timeline": build_timeline(audio_segments, visual_segments),
    }


def serialize(transcript):
    """Stable JSON bytes: the same transcript always gives the same bytes, so a hash can tell
    whether a rebuild changed anything."""
    return json.dumps(transcript, ensure_ascii=False, indent=2).encode("utf-8")


def content_sha256(data):
    return hashlib.sha256(data).hexdigest()
