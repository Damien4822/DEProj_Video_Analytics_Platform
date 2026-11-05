"""Check that the models the pipeline needs are on this machine, and optionally download them.

    python scripts/check_models.py              # report only, changes nothing
    python scripts/check_models.py --download   # fetch the Hugging Face models that are missing

Checks the Hugging Face cache (HF_HOME, default ~/.cache/huggingface) for the Whisper, Florence-2 and
embedding models named by WHISPER_MODEL, VISION_MODEL and EMBED_MODEL, and asks the Ollama server
whether LLM_MODEL has been pulled. Exits with 1 when something is missing, so scripts can use it.
RapidOCR needs no download (its models ship inside the pip package). Run it inside a worker
container to check the `models` volume: docker compose --profile workers run --rm embedder python scripts/check_models.py
"""
import argparse
import json
import sys
import urllib.error
import urllib.request

from vidpipe.config import cfg


def whisper_repo(name):
    """The Hugging Face repo faster-whisper loads for a model size such as 'small'."""
    return name if "/" in name else f"Systran/faster-whisper-{name}"


def hf_models():
    """(role, repo id) of the Hugging Face models the pipeline uses."""
    return [
        ("audio worker (Whisper)", whisper_repo(cfg.whisper_model)),
        ("vision worker (Florence-2)", cfg.vision_model),
        ("embedder", cfg.embed_model),
    ]


def cached_repos():
    """{repo id: size in bytes} of the models in the local Hugging Face cache."""
    from huggingface_hub import scan_cache_dir

    try:
        return {r.repo_id: r.size_on_disk for r in scan_cache_dir().repos if r.repo_type == "model"}
    except Exception:   # no cache directory yet
        return {}


def ollama_models():
    """Names of the models the Ollama server has pulled, or None when it cannot be reached."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))   # local server: no proxy
    try:
        with opener.open(f"{cfg.llm_base_url.rstrip('/')}/api/tags", timeout=5) as response:
            return [m["name"] for m in json.load(response)["models"]]
    except (urllib.error.URLError, OSError, ValueError, KeyError):
        return None


def ollama_has(pulled, wanted):
    return wanted in pulled or (":" not in wanted and f"{wanted}:latest" in pulled)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--download", action="store_true", help="download the missing Hugging Face models")
    args = parser.parse_args()

    missing = False
    cache = cached_repos()
    for role, repo in hf_models():
        if repo not in cache and args.download:
            from huggingface_hub import snapshot_download

            print(f"downloading {repo} ...")
            snapshot_download(repo)
            cache = cached_repos()
        if repo in cache:
            print(f"ok       {repo:<36} {cache[repo] / 1e6:7.0f} MB  {role}")
        else:
            missing = True
            print(f"MISSING  {repo:<36} {'':>10}  {role}")

    pulled = ollama_models()
    if pulled is None:
        missing = True
        print(f"MISSING  {cfg.llm_model:<36} {'':>10}  summarizer: Ollama not reachable at {cfg.llm_base_url}")
    elif ollama_has(pulled, cfg.llm_model):
        print(f"ok       {cfg.llm_model:<36} {'':>10}  summarizer (Ollama)")
    else:
        missing = True
        print(f"MISSING  {cfg.llm_model:<36} {'':>10}  summarizer: run `ollama pull {cfg.llm_model}`")

    if missing:
        print("\nsome models are missing" + ("" if args.download else "; run with --download for the Hugging Face ones"))
    sys.exit(1 if missing else 0)


if __name__ == "__main__":
    main()
