"""Text embeddings for chunks and search queries (sentence-transformers).

One class holds the model so a worker loads it once. The heavy import happens when the model is
loaded, so importing this module stays cheap. Vectors are normalised, so cosine similarity and
dot product agree.
"""
import logging

from vidpipe.config import cfg

log = logging.getLogger(__name__)

# (query prefix, passage prefix) for models trained with instructions, matched on the model name.
# The more specific name goes first.
_PREFIXES = (
    ("bge-m3", ("", "")),
    ("bge-", ("Represent this sentence for searching relevant passages: ", "")),
    ("e5-", ("query: ", "passage: ")),
)


def prefixes_for(model_id):
    """(query prefix, passage prefix) the model expects; empty strings for models without any."""
    name = model_id.lower()
    for key, pair in _PREFIXES:
        if key in name:
            return pair
    return ("", "")


class TextEmbedder:
    def __init__(self, model_id=None, device=None, batch_size=None):
        self.model_id = model_id or cfg.embed_model
        self.device = device or cfg.embed_device
        self.batch_size = batch_size or cfg.embed_batch_size
        self.query_prefix, self.passage_prefix = prefixes_for(self.model_id)
        self._model = None

    def load(self):
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            log.info("loading %s on %s", self.model_id, self.device)
            self._model = SentenceTransformer(self.model_id, device=self.device)
        return self._model

    def _encode(self, texts):
        vectors = self.load().encode(
            texts, batch_size=self.batch_size, normalize_embeddings=True, convert_to_numpy=True,
        )
        return [[float(v) for v in row] for row in vectors]

    def embed_passages(self, texts):
        """Vectors for chunk texts, one list of floats per text."""
        return self._encode([self.passage_prefix + t for t in texts])

    def embed_query(self, text):
        """Vector for a search query."""
        return self._encode([self.query_prefix + text])[0]
