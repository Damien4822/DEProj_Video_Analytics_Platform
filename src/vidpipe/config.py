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
