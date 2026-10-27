"""Frame extractor: video.stored -> keyframe images in object storage -> one frame.extracted each.

    python -m services.frame_extractor.worker    (run from the local/ folder)
"""
import logging
import tempfile
from pathlib import Path

import cv2

from vidpipe import db, storage
from vidpipe.config import cfg
from vidpipe.kafka import run_worker
from vidpipe.keyframes import iter_keyframes
from vidpipe.schemas import FrameExtracted, Topics, VideoStored

STAGE = "frames"
log = logging.getLogger(__name__)


def handle(msg: VideoStored):
    with tempfile.TemporaryDirectory() as tmp:
        video = storage.download_file(msg.video_uri, Path(tmp) / msg.filename)

        frames = []   # (frame_idx, start_s, uri, width, height)
        for start_s, frame in iter_keyframes(video, cfg.sample_fps, cfg.diff_threshold):
            idx = len(frames)
            jpg = Path(tmp) / f"{idx:06d}.jpg"
            cv2.imwrite(str(jpg), frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
            # The key is derived from the frame index, so a re-run overwrites the same objects.
            uri = storage.upload_file(jpg, f"frames/{msg.video_id}/{idx:06d}.jpg")
            jpg.unlink()
            height, width = frame.shape[:2]
            frames.append((idx, start_s, uri, width, height))

    if not frames:
        raise ValueError(f"no frames could be decoded from {msg.filename}")

    # The count is stored and sent with every frame so a consumer can tell when all have arrived.
    with db.connect() as conn:
        db.upsert_video(conn, msg.video_id, keyframe_count=len(frames))
    log.info("extracted %d keyframes from %s", len(frames), msg.video_id)

    return [
        FrameExtracted(
            video_id=msg.video_id, run_id=msg.run_id, frame_idx=idx, start_s=round(start_s, 3),
            frame_uri=uri, frame_count=len(frames), width=width, height=height,
        )
        for idx, start_s, uri, width, height in frames
    ]


def main():
    run_worker(
        stage=STAGE,
        group_id="vidpipe-frames",       # its own group: it gets every video.stored, like the audio worker
        in_topic=Topics.VIDEO_STORED,
        in_model=VideoStored,
        out_topic=Topics.FRAME_EXTRACTED,
        handler=handle,
        max_attempts=1,
    )


if __name__ == "__main__":
    main()
