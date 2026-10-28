"""Short summary of a video from its transcript document (see timeline.py).

The transcript is turned into timestamped notes, one line per keyframe, and a local LLM writes
what is going on. A video whose notes do not fit one LLM call is summarised in parts, and the
partial summaries are summarised again (map-reduce). The LLM is passed in, so this module does
no I/O and tests use a fake.
"""
from vidpipe.chunking import overlaps

MAX_ROUNDS = 4

SYSTEM = (
    "You describe videos from machine-generated notes: what is said, what is shown on screen, "
    "and what text appears on screen, each with a timestamp. Use only the notes. Do not invent "
    "details. Notes can contain recognition errors; ignore anything that does not make sense."
)

PART_PROMPT = """These notes cover one part of a video. Write 2-4 sentences on what happens in this part.
Keep the timestamps of the important moments, in the form [mm:ss].

Notes:
{notes}"""

FINAL_PROMPT = """These notes cover a whole video. Write:
1. A short summary of what is going on in the video (3-5 sentences).
2. Then up to 5 bullet points for the key moments, each starting with its timestamp, like "- [01:23] ...".

Notes:
{notes}"""


def fmt_time(seconds):
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes:02d}:{secs:02d}"


def render_notes(transcript):
    """One line per keyframe segment: [start-end] Scene / On-screen / Speech.

    A sentence that spans several keyframes is written once, on the first one, so the LLM does
    not read the same speech repeatedly (the transcript's `timeline` repeats it).
    """
    audio = transcript["audio_segments"]
    entries = transcript["timeline"] or [
        {"start": 0.0, "end": transcript["duration"], "caption": "", "ocr_text": ""}]
    used, lines = set(), []
    for entry in entries:
        speech = []
        for seg in audio:
            if seg["segment_id"] not in used and overlaps(seg["start"], seg["end"],
                                                          entry["start"], entry["end"]):
                used.add(seg["segment_id"])
                speech.append(seg["text"].strip())
        parts = [f"{label}: {value}" for label, value in (
            ("Scene", entry["caption"].strip()), ("On-screen", entry["ocr_text"].strip()),
            ("Speech", " ".join(speech))) if value]
        if parts:
            lines.append(f"[{fmt_time(entry['start'])}-{fmt_time(entry['end'])}] " + ". ".join(parts))
    return lines


def pack(lines, max_chars):
    """Group lines into batches of at most max_chars (a single longer line is its own batch)."""
    batches, current, size = [], [], 0
    for line in lines:
        if current and size + len(line) + 1 > max_chars:
            batches.append(current)
            current, size = [], 0
        current.append(line)
        size += len(line) + 1
    if current:
        batches.append(current)
    return batches


def summarize(transcript, llm, max_chars):
    """Return the summary text. llm needs generate(prompt, system) -> str."""
    lines = render_notes(transcript)
    if not lines:
        raise ValueError("the transcript has nothing to summarise (no speech, captions or text)")
    batches = pack(lines, max_chars)
    for _ in range(MAX_ROUNDS):
        if len(batches) == 1:
            return llm.generate(FINAL_PROMPT.format(notes="\n".join(batches[0])), SYSTEM)
        partials = [llm.generate(PART_PROMPT.format(notes="\n".join(b)), SYSTEM) for b in batches]
        batches = pack(partials, max_chars)
    raise RuntimeError(f"the notes did not shrink to one batch in {MAX_ROUNDS} rounds")
