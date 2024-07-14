from sentence_transformers import SentenceTransformer
from rocksdict import Rdict, AccessType
import numpy as np


def get_n_docs(docs, n: int):
    """
    returns n documents of the collection of documents

    :param docs: iterable of documents with url and value
    :param n: number of documents to return
    :returns: list of n documents or less if the iterable has less than n documents
    """
    counter = 0
    pulled_docs = []
    while counter < n:
        try:
            _, next_doc = next(docs)
            # check if next_doc actually has value
            if next_doc != "" and next_doc is not None:
                pulled_docs.append(next_doc)
                counter += 1
        except StopIteration:
            break

    return pulled_docs


def generate_embeddings(db_path: str, embeddings_path: str, batch_size=1000):
    """
    Generates embedding vectors for all documents in the rocksdict database.
    The sentence_transformer model all-mpnet-base-v2 is used to generate the embeddings.
    The embeddings are saved as separate files for each batch of documents.

    :param db_path: path to rocksdict database
    :param embeddings_path: path to the directory where the embeddings should be stored
    :param batch_size: number of documents that are processed in one batch and saved in one file
    :return: None
    """
    model = SentenceTransformer("all-mpnet-base-v2", device="cuda")
    # loading the dictionary database
    db = Rdict(db_path, access_type = AccessType.read_only())
    docs_iterator = iter(db.values())
    new_docs_available = True
    batch_counter = 0

    while new_docs_available:
        next_docs = get_n_docs(docs_iterator, batch_size)
        new_docs_available = len(next_docs) == batch_size
        embeddings = model.encode(next_docs)
        batch_counter += 1
        # each batch is saved alone to counter system failures
        np.save(f"{embeddings_path}/embeddings_{batch_counter}.npy", embeddings)
        print(f"{batch_counter * batch_size} docs processed")


def combine_embeddings(embeddings_path: str, combined_embeddings_name: str, number_of_batches: int):
    """
    Combines the embedding vectors of all batches into a single file.
    The combined embeddings file is saved in the same directory as the separate embeddings file.

    :param embeddings_path: path to the directory where the embeddings are stored
    :param combined_embeddings_name: name of the combined embeddings file without file extension
    :param number_of_batches: number of batches to combine

    :return: None
    """
    concat_array = np.load(f"{embeddings_path}/embeddings_{1}.npy")

    for batch_index in range(2, number_of_batches + 1):
        new_array = np.load(f"{embeddings_path}/embeddings_{batch_index}.npy")
        concat_array = np.concatenate((concat_array, new_array), axis=0)

    np.save(f"{embeddings_path}/{combined_embeddings_name}.npy", concat_array)


if __name__ == '__main__':
    pass
