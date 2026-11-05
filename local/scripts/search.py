"""Semantic search over the embedded chunks.

    python scripts/search.py "who talks about stress at work"
    python scripts/search.py "stress" --video <video_id> --limit 10

Run from the local/ folder (or inside a worker container). The query is embedded with the same
model the chunks were embedded with (EMBED_MODEL), then compared by cosine similarity.
"""
import argparse

from vidpipe import db
from vidpipe.models.embedder import TextEmbedder
from vidpipe.summary import fmt_time


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("query")
    parser.add_argument("--video", help="only search this video_id")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--model", help="embedding model to search (default: EMBED_MODEL)")
    args = parser.parse_args()

    embedder = TextEmbedder(model_id=args.model)
    vector = embedder.embed_query(args.query)
    with db.connect() as conn:
        rows = db.search_chunks(conn, vector, embedder.model_id, limit=args.limit, video_id=args.video)

    if not rows:
        print(f"no embedded chunks for model {embedder.model_id}: is the embedder running?")
        return
    for r in rows:
        title = r["title"] or r["video_id"]
        print(f"{r['score']:.3f}  {title}  [{fmt_time(r['start_s'])}-{fmt_time(r['end_s'])}]")
        print("       " + r["text"].replace("\n", " ")[:240])


if __name__ == "__main__":
    main()
