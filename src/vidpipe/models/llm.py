"""Client for a local LLM served by Ollama (plain HTTP, no extra Python package).

Any model Ollama can serve works: change LLM_MODEL. Another runtime with a different API only
needs a class with the same generate() method.
"""
import json
import logging
import urllib.error
import urllib.request

from vidpipe.config import cfg

log = logging.getLogger(__name__)


class OllamaClient:
    def __init__(self, base_url=None, model=None, num_ctx=None, timeout_s=None):
        self.base_url = (base_url or cfg.llm_base_url).rstrip("/")
        self.model = model or cfg.llm_model
        self.num_ctx = num_ctx or cfg.llm_num_ctx
        self.timeout_s = timeout_s or cfg.llm_timeout_s
        # A local server must not go through a corporate proxy.
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def generate(self, prompt, system=None):
        body = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.2, "num_ctx": self.num_ctx},
        }
        if system:
            body["system"] = system
        request = urllib.request.Request(
            f"{self.base_url}/api/generate", data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        try:
            with self._opener.open(request, timeout=self.timeout_s) as response:
                text = json.load(response)["response"].strip()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            raise RuntimeError(
                f"Ollama returned {exc.code} for model {self.model!r} (is it pulled?): {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"cannot reach Ollama at {self.base_url} (is it running?): {exc.reason}") from exc
        if not text:
            raise RuntimeError(f"model {self.model!r} returned an empty answer")
        return text
