"""Audio worker: video.stored -> faster-whisper -> audio_segments rows -> audio.done.

    python -m services.audio_worker.worker    (run from the local/ folder)
"""
import logging
import tempfile
from pathlib import Path

import ffmpeg
from faster_whisper import WhisperModel

from vidpipe import db, storage
from vidpipe.config import cfg
from vidpipe.kafka import run_worker
from vidpipe.quality import clean_segments
from vidpipe.schemas import AudioDone, Topics, VideoStored

STAGE = "audio"
log = logging.getLogger(__name__)

_model = None


def get_model():
    """Load the model once per process (slow), not once per message."""
    global _model
    if _model is None:
        log.info("loading whisper model %s (%s, %s)", cfg.whisper_model, cfg.whisper_device,
                 cfg.whisper_compute_type)
        _model = WhisperModel(cfg.whisper_model, device=cfg.whisper_device,
                              compute_type=cfg.whisper_compute_type)
    return _model


def has_audio(path):
    return any(s["codec_type"] == "audio" for s in ffmpeg.probe(str(path))["streams"])


def extract_audio(video_path, wav_path):
    # Mono, 16 kHz wav: what Whisper expects.
    (
        ffmpeg.input(str(video_path))
        .output(str(wav_path), vn=None, ac=1, ar=16000)
        .overwrite_output()
        .run(quiet=True)
    )


def handle(msg: VideoStored) -> AudioDone:
    with tempfile.TemporaryDirectory() as tmp:
        video = storage.download_file(msg.video_uri, Path(tmp) / msg.filename)

        if not has_audio(video):
            log.warning("video %s has no audio stream", msg.video_id)
            with db.connect() as conn:
                db.replace_audio_segments(conn, msg.video_id, [], cfg.whisper_model)
                db.upsert_video(conn, msg.video_id, audio_segment_count=0)
            return AudioDone(video_id=msg.video_id, run_id=msg.run_id, duration_s=msg.duration_s,
                             segment_count=0, asr_model=cfg.whisper_model)

        wav = Path(tmp) / "audio.wav"
        extract_audio(video, wav)

        segments, info = get_model().transcribe(str(wav))
        raw = [{"start": s.start, "end": s.end, "text": s.text} for s in segments]
        cleaned, issues = clean_segments(raw, msg.duration_s)
        for issue in issues:
            log.warning("quality %s: %s", msg.video_id, issue)

        audio_uri = storage.upload_file(wav, f"audio/{msg.video_id}/audio.wav")

    with db.connect() as conn:
        db.replace_audio_segments(conn, msg.video_id, cleaned, cfg.whisper_model)
        # audio_segment_count doubles as the "audio is finished" flag the chunk builder checks
        db.upsert_video(conn, msg.video_id, language=info.language, audio_segment_count=len(cleaned))
    log.info("transcribed %s: %d segments, language %s", msg.video_id, len(cleaned), info.language)

    return AudioDone(
        video_id=msg.video_id, run_id=msg.run_id, audio_uri=audio_uri,
        duration_s=msg.duration_s, segment_count=len(cleaned), asr_model=cfg.whisper_model,
        language=info.language, language_probability=info.language_probability,
    )


def main():
    run_worker(
        stage=STAGE,
        group_id="vidpipe-audio",
        in_topic=Topics.VIDEO_STORED,
        in_model=VideoStored,
        out_topic=Topics.AUDIO_DONE,
        handler=handle,
        max_attempts=1,   # a transcription failure is rarely transient; do not repeat a long job
    )


if __name__ == "__main__":
    main()
