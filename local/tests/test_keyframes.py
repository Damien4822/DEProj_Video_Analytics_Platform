import numpy as np
import pytest

cv2 = pytest.importorskip("cv2")

from vidpipe.keyframes import iter_keyframes  # noqa: E402


def make_video(path, colors, seconds_each=3, fps=10, size=(160, 90)):
    writer = cv2.VideoWriter(str(path), cv2.VideoWriter_fourcc(*"mp4v"), fps, size)
    for color in colors:
        frame = np.full((size[1], size[0], 3), color, dtype=np.uint8)
        for _ in range(seconds_each * fps):
            writer.write(frame)
    writer.release()


def test_a_scene_change_produces_a_new_keyframe(tmp_path):
    video = tmp_path / "v.mp4"
    make_video(video, [0, 255])                       # 3 s black, then 3 s white
    frames = list(iter_keyframes(video, sample_fps=1, diff_threshold=8.0))
    times = [round(t) for t, _ in frames]
    assert times == [0, 3]


def test_a_static_video_has_one_keyframe(tmp_path):
    video = tmp_path / "v.mp4"
    make_video(video, [128])
    assert len(list(iter_keyframes(video, sample_fps=1, diff_threshold=8.0))) == 1


def test_a_high_threshold_ignores_small_changes(tmp_path):
    video = tmp_path / "v.mp4"
    make_video(video, [100, 110])                     # a mean change of about 10
    assert len(list(iter_keyframes(video, 1, diff_threshold=8.0))) == 2
    assert len(list(iter_keyframes(video, 1, diff_threshold=50.0))) == 1


def test_an_unreadable_file_raises(tmp_path):
    bad = tmp_path / "bad.mp4"
    bad.write_bytes(b"not a video")
    with pytest.raises(ValueError):
        list(iter_keyframes(bad, 1, 8.0))
