### Code for the retrieval model and the BM25 inverted index

The `bm25.py` file contains the code for the lexical retriever that makes use of an inverted index. The `naive_retriever.py` file contains the code for the nearest-neighbors lookup based on sentence transformer embeddings. `combined_results.py` combines the results from both the retrievers using a weighted sum of ranks.