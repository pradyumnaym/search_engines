import torch
import numpy as np
from sentence_transformers import SentenceTransformer
from rocksdict import Rdict, AccessType

KEEP_MODEL_IN_GPU = True
if KEEP_MODEL_IN_GPU:
    model = SentenceTransformer("all-mpnet-base-v2", device="cuda")

loaded_embeddings = np.load("../data/embeddings/combined_embeddings.npy").astype(np.float16)


def naive_k_nearest_neighbor_search(embeddings: np.ndarray, query: np.ndarray, k: int,
                                    device=torch.device("cpu")) -> tuple[np.ndarray, np.ndarray]:
    """
    A naive implementation of k-nearest neighbor query_postprocessing. All vectors in embeddings are compared to the query vector.
    For large datasets this can be too much. But for a small dataset < 3 million it works pretty good.

    :param embeddings: the embedding vectors of the corpus.
    :param query: query embedded as vector
    :param k: the number of nearest neighbors to return
    :param device: (optional) the device to use for the computation (standard is cpu)

    :return: a tuple of the distance of the nearest neighbors and the indices of the nearest neighbors.
    """
    torch_embeddings = torch.from_numpy(embeddings).to(device)
    torch_query = torch.from_numpy(query).to(device)

    distance = torch.sum(torch.square(torch_embeddings - torch_query), 1)
    top_k_values, top_k_indices = torch.topk(distance, k, largest=False)
    return top_k_values.numpy(), top_k_indices.numpy()


def get_result(query: str, n_results: int, db_path="../data/new_db"):
    """
    Returns the top {n_results} results for the given query.
    The query is embedded and then a k_nearest neighbor query_postprocessing is performed.
    And the results are retrieved from the database

    :param query: query as a string :param n_results: (optional) number of results to return (standard is 100)
    :param n_results: (optional) number of results to return (standard is 100)
    :param db_path: (optional) the path to the database (standard is "../data/new_db")

    :return: the top {n_results} results for the given query, consisting of the url and actual document
    """
    # if not KEEP_MODEL_IN_GPU:
    #     model = SentenceTransformer("all-mpnet-base-v2", device="cuda")

    db = Rdict(db_path, access_type = AccessType.read_only())

    embedded_query = model.encode(query).astype(np.float16)

    # if not KEEP_MODEL_IN_GPU:
    #     del model

    top_k_scores, top_k_indices = naive_k_nearest_neighbor_search(loaded_embeddings, embedded_query, n_results)
    result_urls = []
    result_scores = top_k_indices.tolist()

    for result_index in result_scores:
        result_urls.append(db[result_index][0])

    return list(zip(result_urls, result_scores))


def print_result(results, query=None):
    """
    Prints the results and optional query to the console.

    :param results: the results for the given query
    :param query: (optional) the query

    :return: None
    """
    if query is not None:
        print(f"Search results for \"{query}\"")
        print("\n")

    for url, doc in results:
        print(url)
        print(doc)
        print("\n------------------------------------------")
        print("\n")


if __name__ == '__main__':
    query1 = "university tuebingen"
    results1 = get_result(query1, n_results=10)
    print_result(results1, query=query1)

    query2 = "Boris Palmer"
    results2 = get_result(query2, n_results=10)
    print_result(results2, query=query2)


