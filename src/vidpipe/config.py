"""Runtime settings, read from environment variables.

Defaults match local/infra/.env.example, and that file is loaded if present, so a worker
started on the same machine as the Docker stack needs no extra setup.
"""
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / "local" / "infra" / ".env")


def _env(name, default):
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    s3_endpoint: str = _env("S3_ENDPOINT", "http://localhost:9000")
    s3_access_key: str = _env("S3_ACCESS_KEY", "vidpipeaccess")
    s3_secret_key: str = _env("S3_SECRET_KEY", "vidpipesecret123")
    s3_bucket: str = _env("S3_BUCKET", "vidpipe")

    postgres_host: str = _env("POSTGRES_HOST", "localhost")
    postgres_port: str = _env("POSTGRES_PORT", "5432")
    postgres_db: str = _env("POSTGRES_DB", "vidpipe")
    postgres_user: str = _env("POSTGRES_USER", "vidpipe")
    postgres_password: str = _env("POSTGRES_PASSWORD", "vidpipe")

    whisper_model: str = _env("WHISPER_MODEL", "small")
    whisper_device: str = _env("WHISPER_DEVICE", "cpu")
    whisper_compute_type: str = _env("WHISPER_COMPUTE_TYPE", "int8")

    # Keyframe selection and vision models (values from the video notebook).
    sample_fps: float = float(_env("SAMPLE_FPS", "1"))
    diff_threshold: float = float(_env("DIFF_THRESHOLD", "8.0"))
    ocr_min_score: float = float(_env("OCR_MIN_SCORE", "0.5"))
    vision_model: str = _env("VISION_MODEL", "microsoft/Florence-2-base")
    vision_device: str = _env("VISION_DEVICE", "cpu")

    # Chunking: window length and overlap in seconds. Changing them makes a new chunk_version.
    chunk_window_s: float = float(_env("CHUNK_WINDOW_S", "45"))
    chunk_overlap_s: float = float(_env("CHUNK_OVERLAP_S", "10"))

    # Local LLM for the summary: any model served by Ollama (https://ollama.com).
    # The context is small on purpose: this runs on a CPU VM with a few GB of free RAM.
    llm_base_url: str = _env("LLM_BASE_URL", "http://localhost:11434")
    llm_model: str = _env("LLM_MODEL", "qwen2.5:3b")
    llm_num_ctx: int = int(_env("LLM_NUM_CTX", "4096"))
    llm_timeout_s: float = float(_env("LLM_TIMEOUT_S", "900"))
    summary_batch_chars: int = int(_env("SUMMARY_BATCH_CHARS", "8000"))   # notes per LLM call

    # Same selector as the Setup notebook: cap the height, not the length.
    ytdlp_format: str = _env("YTDLP_FORMAT", "bv*[height<=720]+ba/b[height<=720]/b")
    # Set to 1 on networks that re-sign HTTPS (use the system CA store instead of certifi).
    ytdlp_no_certifi: bool = _env("YTDLP_NO_CERTIFI", "0") == "1"

    @property
    def postgres_dsn(self):
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user={self.postgres_user} password={self.postgres_password}"
        )


cfg = Settings()
