-- Chunk embeddings (pgvector). Vectors live in their own table so the embedding model can change
-- without touching the chunks. The column has no fixed dimension, so vectors of different models
-- (different sizes) sit side by side; (chunk_id, model) is the key.
--
-- Deleting or rebuilding a chunk deletes its embeddings (ON DELETE CASCADE), so a changed chunk
-- is always embedded again.

CREATE TABLE IF NOT EXISTS chunk_embeddings (
    chunk_id    uuid NOT NULL REFERENCES chunks (chunk_id) ON DELETE CASCADE,
    model       text NOT NULL,                       -- e.g. BAAI/bge-small-en-v1.5
    dim         integer NOT NULL CHECK (dim > 0),
    embedding   vector NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chunk_id, model),
    CHECK (vector_dims(embedding) = dim)
);

-- An HNSW index needs a fixed dimension, so it is an expression index on a cast, limited to one
-- model. Searches must use the same cast and model filter (see db.search_chunks). To index another
-- model, add one more index with its name and dimension. Without an index a search still works
-- (a sequential scan), which is fine for a small collection.
CREATE INDEX IF NOT EXISTS idx_chunk_embeddings_bge_small ON chunk_embeddings
    USING hnsw ((embedding::vector(384)) vector_cosine_ops)
    WHERE model = 'BAAI/bge-small-en-v1.5';
