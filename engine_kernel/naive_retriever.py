import torch
import numpy as np
from sentence_transformers import SentenceTransformer
from rocksdict import Rdict, AccessType

model = SentenceTransformer("all-mpnet-base-v2", device="cuda")

loaded_embeddings = np.load("../data/embeddings/combined_embedding_2.npy").astype(np.float16)
print("model and embeddings loaded")


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

    cosine_distance = torch.tensordot(torch_query, torch_embeddings.T, dims=1)
    top_k_values, top_k_embeddings = torch.topk(cosine_distance, k, largest=True)
    return top_k_values.numpy(), top_k_embeddings.numpy()


def get_result(query: str, n_results: int, embeddings_db_path="../data/embeddings_db"):
    """
    Returns the top {n_results} results for the given query.
    The query is embedded and then a k_nearest neighbor query_postprocessing is performed.
    And the results are retrieved from the database

    :param query: query as a string :param n_results: (optional) number of results to return (standard is 100)
    :param n_results: (optional) number of results to return (standard is 100)
    :param embeddings_db_path: (optional) the path to the database (standard is "../data/new_db")

    :return: the top {n_results} results for the given query, consisting of the url and actual document
    """
    embeddings_db = Rdict(embeddings_db_path, access_type=AccessType.read_only())

    embedded_query = model.encode(query).astype(np.float16)

    top_k_scores, top_k_embeddings = naive_k_nearest_neighbor_search(loaded_embeddings, embedded_query, n_results)
    embedding_indices = top_k_embeddings.tolist()
    result_indices = list(map(lambda x: embeddings_db[x], embedding_indices))

    seen_indices = []
    result = []
    for index, score in zip(result_indices, top_k_scores):
        if index not in seen_indices:
            result.append((index, score))
            seen_indices.append(index)

    return result


