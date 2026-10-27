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


def chunk_version_for(window_s, overlap_s):
    """Names the chunking parameters, so different settings produce separate, comparable chunk sets."""
    return f"w{window_s:g}-o{overlap_s:g}"


def chunk_id_for(video_id, chunk_version, start_s):
    """Deterministic: rebuilding the same chunk gives the same id, so writes are idempotent."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{video_id}|{chunk_version}|{round(start_s * 1000)}"))
