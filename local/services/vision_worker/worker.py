"""Vision worker: frame.extracted -> Florence-2 caption/objects + OCR -> rows -> frame.described.

Run several copies to speed up a video; Kafka splits the frame.extracted partitions between them.

    python -m services.vision_worker.worker    (run from the local/ folder)
"""
import logging
import tempfile
from pathlib import Path

import cv2

from vidpipe import db, storage
from vidpipe.config import cfg
from vidpipe.kafka import run_worker
from vidpipe.models.frame_describer import FrameDescriber
from vidpipe.quality import clean_boxes
from vidpipe.schemas import FrameDescribed, FrameExtracted, Topics

STAGE = "vision"
log = logging.getLogger(__name__)

describer = FrameDescriber()   # models load on the first frame, once per process


def handle(msg: FrameExtracted) -> FrameDescribed:
    with tempfile.TemporaryDirectory() as tmp:
        path = storage.download_file(msg.frame_uri, Path(tmp) / "frame.jpg")
        frame = cv2.imread(str(path))
    if frame is None:
        raise ValueError(f"cannot decode frame image {msg.frame_uri}")

    result = describer.describe(frame)
    caption = result["caption"].strip()
    objects, issues = clean_boxes(result["objects"], msg.width, msg.height, "label")
    ocr, ocr_issues = clean_boxes(result["ocr"], msg.width, msg.height, "text")
    for issue in issues + ocr_issues:
        log.warning("quality %s frame %d: %s", msg.video_id, msg.frame_idx, issue)

    with db.connect() as conn:
        db.replace_visual_frame(
            conn, msg.video_id, msg.frame_idx, msg.start_s, msg.frame_uri, caption,
            cfg.vision_model, objects, ocr,
        )
    log.info("described %s frame %d/%d at %.1fs: %s", msg.video_id, msg.frame_idx + 1,
             msg.frame_count, msg.start_s, caption)

    return FrameDescribed(
        video_id=msg.video_id, run_id=msg.run_id, frame_idx=msg.frame_idx, start_s=msg.start_s,
        frame_count=msg.frame_count, caption=caption, object_count=len(objects),
        ocr_count=len(ocr), vision_model=cfg.vision_model,
    )


def main():
    run_worker(
        stage=STAGE,
        group_id="vidpipe-vision",
        in_topic=Topics.FRAME_EXTRACTED,
        in_model=FrameExtracted,
        out_topic=Topics.FRAME_DESCRIBED,
        handler=handle,
        track_run=False,     # one processing_runs row per frame would be noise; failures are still recorded
    )


if __name__ == "__main__":
    main()
