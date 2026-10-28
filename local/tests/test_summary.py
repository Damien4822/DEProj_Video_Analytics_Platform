import pytest

from vidpipe.summary import fmt_time, pack, render_notes, summarize
from vidpipe.timeline import build_transcript

VIDEO = {"video_id": "vid", "duration_s": 60.0, "language": "en"}
AUDIO = [
    {"segment_idx": 0, "start_s": 0.0, "end_s": 10.0, "text": "first sentence"},
    {"segment_idx": 1, "start_s": 10.0, "end_s": 20.0, "text": "a long second sentence"},
]
VISUAL = [
    {"frame_idx": 0, "start_s": 0.0, "end_s": 15.0, "caption": "a logo", "objects": [],
     "ocr": [{"text": "TED", "score": 0.9, "bbox": [0, 0, 1, 1]}]},
    {"frame_idx": 1, "start_s": 15.0, "end_s": 60.0, "caption": "a man on a stage", "objects": [], "ocr": []},
]


class FakeLLM:
    def __init__(self):
        self.prompts = []

    def generate(self, prompt, system=None):
        self.prompts.append(prompt)
        return f"answer {len(self.prompts)}"


def transcript():
    return build_transcript(VIDEO, AUDIO, VISUAL)


def test_times_are_formatted_as_mm_ss_or_h_mm_ss():
    assert (fmt_time(0), fmt_time(75.9), fmt_time(3725)) == ("00:00", "01:15", "1:02:05")


def test_notes_have_one_line_per_keyframe_with_timestamps():
    lines = render_notes(transcript())
    assert lines[0] == "[00:00-00:15] Scene: a logo. On-screen: TED. Speech: first sentence a long second sentence"
    assert lines[1] == "[00:15-01:00] Scene: a man on a stage"


def test_a_sentence_spanning_keyframes_is_written_once():
    text = "\n".join(render_notes(transcript()))
    assert text.count("a long second sentence") == 1


def test_pack_respects_the_size_limit_and_keeps_order():
    batches = pack(["aaaa", "bbbb", "cccc"], 10)
    assert batches == [["aaaa", "bbbb"], ["cccc"]]
    assert pack(["x" * 50], 10) == [["x" * 50]]      # one long line is not split
    assert pack([], 10) == []


def test_a_short_video_takes_one_llm_call():
    llm = FakeLLM()
    assert summarize(transcript(), llm, 10_000) == "answer 1"
    assert len(llm.prompts) == 1
    assert "[00:15-01:00] Scene: a man on a stage" in llm.prompts[0]


def test_a_long_video_is_summarised_in_parts_then_combined():
    llm = FakeLLM()
    assert summarize(transcript(), llm, 60) == "answer 3"       # two parts, then the final summary
    assert len(llm.prompts) == 3
    assert "answer 1" in llm.prompts[2] and "answer 2" in llm.prompts[2]


def test_a_transcript_with_nothing_in_it_is_rejected():
    empty = build_transcript(VIDEO, [], [])
    empty["timeline"] = []
    with pytest.raises(ValueError):
        summarize(empty, FakeLLM(), 1000)


def test_an_llm_that_never_shrinks_the_notes_fails_instead_of_looping():
    class Wordy(FakeLLM):
        def generate(self, prompt, system=None):
            return "x" * 200
    with pytest.raises(RuntimeError):
        summarize(transcript(), Wordy(), 50)
