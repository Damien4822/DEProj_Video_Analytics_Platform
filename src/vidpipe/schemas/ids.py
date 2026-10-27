"""Deterministic identifiers: the same input always maps to the same video_id."""
import uuid
from urllib.parse import urldefrag


def video_id_for_url(url):
    url, _ = urldefrag(url.strip())
    return str(uuid.uuid5(uuid.NAMESPACE_URL, url))


def video_id_for_sha256(sha256_hex):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"sha256:{sha256_hex}"))


def new_run_id():
    return str(uuid.uuid4())
