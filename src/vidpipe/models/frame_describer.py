"""Frame description: caption and objects (Florence-2) plus on-screen text (RapidOCR).

One class holds the models so a worker loads them once. The heavy imports are done when the
models are loaded, so importing this module stays cheap. Other runtimes (a Spark job, a
notebook) can reuse it unchanged.
"""
import logging

from vidpipe.config import cfg

log = logging.getLogger(__name__)


class FrameDescriber:
    def __init__(self, model_id=None, device=None, ocr_min_score=None):
        self.model_id = model_id or cfg.vision_model
        self.device = device or cfg.vision_device
        self.ocr_min_score = cfg.ocr_min_score if ocr_min_score is None else ocr_min_score
        self._loaded = False

    def load(self):
        if self._loaded:
            return
        import torch
        from rapidocr_onnxruntime import RapidOCR
        from transformers import AutoModelForCausalLM, AutoProcessor

        log.info("loading %s on %s", self.model_id, self.device)
        self._torch = torch
        # eager attention and use_cache=False work around bugs with the pinned remote code
        self._model = AutoModelForCausalLM.from_pretrained(
            self.model_id, trust_remote_code=True, torch_dtype=torch.float32,
            attn_implementation="eager",
        ).to(self.device).eval()
        self._processor = AutoProcessor.from_pretrained(self.model_id, trust_remote_code=True)
        self._ocr = RapidOCR()
        self._loaded = True

    def _run_florence(self, task, image):
        inputs = self._processor(text=task, images=image, return_tensors="pt").to(self.device)
        with self._torch.inference_mode():
            ids = self._model.generate(
                input_ids=inputs["input_ids"],
                pixel_values=inputs["pixel_values"],
                max_new_tokens=128,
                num_beams=1,        # greedy: much faster on CPU than beam search
                do_sample=False,
                use_cache=False,
            )
        text = self._processor.batch_decode(ids, skip_special_tokens=False)[0]
        return self._processor.post_process_generation(
            text, task=task, image_size=(image.width, image.height)
        )

    def _run_ocr(self, frame_bgr):
        result, _ = self._ocr(frame_bgr)
        items = []
        for polygon, text, score in result or []:
            if float(score) < self.ocr_min_score:
                continue
            xs = [p[0] for p in polygon]
            ys = [p[1] for p in polygon]
            items.append({
                "text": text,
                "score": round(float(score), 3),
                "bbox": [round(min(xs)), round(min(ys)), round(max(xs)), round(max(ys))],
            })
        return items

    def describe(self, frame_bgr):
        """Describe one BGR frame: {"caption", "objects": [{label, bbox}], "ocr": [{text, score, bbox}]}."""
        import cv2
        from PIL import Image

        self.load()
        image = Image.fromarray(cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB))
        caption = self._run_florence("<CAPTION>", image)["<CAPTION>"]
        od = self._run_florence("<OD>", image)["<OD>"]
        return {
            "caption": caption,
            "objects": [
                {"label": label, "bbox": [round(v) for v in box]}
                for label, box in zip(od["labels"], od["bboxes"])
            ],
            "ocr": self._run_ocr(frame_bgr),
        }
