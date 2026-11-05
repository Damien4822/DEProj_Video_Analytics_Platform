from vidpipe.db import vector_literal
from vidpipe.models.embedder import TextEmbedder, prefixes_for
from vidpipe.schemas import ChunkEmbedded, Topics


def test_vector_literal_is_a_pgvector_text_value():
    assert vector_literal([0.5, -1, 0.25]) == "[0.5,-1,0.25]"
    assert vector_literal([1 / 3]) == "[0.33333333]"


def test_prefixes_follow_the_model_name():
    bge_query, bge_passage = prefixes_for("BAAI/bge-small-en-v1.5")
    assert "search" in bge_query and bge_passage == ""
    assert prefixes_for("BAAI/bge-m3") == ("", "")                       # more specific than bge-
    assert prefixes_for("intfloat/multilingual-e5-small") == ("query: ", "passage: ")
    assert prefixes_for("some/other-model") == ("", "")


def test_embedder_applies_the_prefixes_without_loading_a_model():
    e = TextEmbedder(model_id="intfloat/e5-small", device="cpu", batch_size=2)
    seen = []
    e._encode = lambda texts: seen.append(texts) or [[0.0] for _ in texts]
    e.embed_passages(["a", "b"])
    e.embed_query("q")
    assert seen == [["passage: a", "passage: b"], ["query: q"]]
    assert e._model is None


def test_chunk_embedded_is_keyed_by_chunk_and_roundtrips():
    msg = ChunkEmbedded(video_id="v", run_id="r", chunk_id="c1", chunk_idx=0, chunk_version="w45-o10",
                        chunk_count=3, model="m", dim=384)
    assert msg.partition_key() == "c1"
    assert ChunkEmbedded.model_validate_json(msg.model_dump_json()) == msg
    assert Topics.CHUNK_EMBEDDED == "chunk.embedded"
