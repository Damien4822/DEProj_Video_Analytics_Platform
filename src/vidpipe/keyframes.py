"""Keyframe selection: sample the video and keep a frame only when the scene changed."""
import cv2
import numpy as np


def iter_keyframes(video_path, sample_fps, diff_threshold):
    """Yield (timestamp_s, frame_bgr) for sampled frames that differ from the last kept one.

    The difference is the mean absolute pixel change on a 64x36 grayscale copy (0-255).
    The first sampled frame is always kept.
    """
    cap = cv2.VideoCapture(str(video_path))
    try:
        fps = cap.get(cv2.CAP_PROP_FPS)
        if not fps or fps <= 0:
            raise ValueError(f"cannot read the frame rate of {video_path}")
        step = max(1, round(fps / sample_fps))
        last_small = None
        idx = 0
        while cap.grab():  # grab() is cheap; only decode (retrieve) the sampled frames
            if idx % step == 0:
                ok, frame = cap.retrieve()
                if ok:
                    small = cv2.cvtColor(cv2.resize(frame, (64, 36)), cv2.COLOR_BGR2GRAY)
                    small = small.astype(np.float32)
                    if last_small is None or np.abs(small - last_small).mean() > diff_threshold:
                        last_small = small
                        yield idx / fps, frame
            idx += 1
    finally:
        cap.release()
