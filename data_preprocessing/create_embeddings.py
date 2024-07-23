from sentence_transformers import SentenceTransformer
from rocksdict import Rdict, AccessType
import numpy as np
from interface import DocInfo


MAX_WINDOW_SIZE = 300
WINDOW_OVERLAP = 7
MAX_WINDOW_NUMBER = 3


def create_doc_windows(single_doc: DocInfo, max_window_number: int, max_window_size: int,
                       window_overlap: int) -> list[DocInfo]:
    """
    Slices a single document into overlapping windows.
    That way a document that is too long can be seperated into smaller windows.

    ----------
    :param single_doc: A document in the form of a DocInfo object.
    :param max_window_number: The maximum number of windows
    :param max_window_size: The maximum number of words of a window
    :param window_overlap: The number of words that are overlapped between windows

    :return: A list of windows
    """
    word_list = single_doc.word_list
    doc_length = len(word_list)
    index_step = max_window_size - window_overlap  # step for each index increase
    window_counter = 0

    doc_windows = []

    for doc_index in range(0, doc_length, index_step):
        window_words = word_list[doc_index:doc_index + max_window_size]

        doc_windows.append(DocInfo(single_doc.doc_index, window_words))
        window_counter += 1
        if window_counter >= max_window_number:
            break

    return doc_windows


def get_n_docs(docs, n: int):
    """
    returns n (or few more) documents of the collection of documents

    :param docs: iterable of DocInfo objects
    :param n: number of documents to return

    :returns: list of n documents. It doesn't have to be exactly n documents. Either the windows inflate it a bit too much.
    Or no more documents are remaining.
    """
    counter = 0
    pulled_docs = []
    while counter < n:
        try:
            next_doc_info: DocInfo = next(docs)
            # check if next_doc actually has value
            if next_doc_info is not None:
                word_list = next_doc_info.word_list
                if len(word_list) > MAX_WINDOW_SIZE:  # if doc is too long it gets sliced into windows
                    doc_windows = create_doc_windows(next_doc_info, MAX_WINDOW_NUMBER, MAX_WINDOW_SIZE, WINDOW_OVERLAP)
                    pulled_docs.extend(doc_windows)
                    counter += len(doc_windows)
                else:
                    pulled_docs.append(next_doc_info)
                    counter += 1

        except StopIteration:
            break

    return pulled_docs


def generate_embeddings(source_db_path: str, embeddings_db_path: str, embeddings_path: str, batch_size=1000) -> int:
    """
    Generates embedding vectors for all documents in the rocksdict database.
    The sentence_transformer model all-mpnet-base-v2 is used to generate the embeddings.
    The indices of the document windows are saved and associated with the doc index in the embeddings database.
    The embeddings are saved as separate files for each batch of documents.

    :param source_db_path: path to db with the original documents
    :param embeddings_db_path: path to db that stores the indices of the embeddings
    :param embeddings_path: path to the directory where the embeddings should be stored
    :param batch_size: number of documents that are processed in one batch and saved in one file
    :return: None
    """
    model = SentenceTransformer("../data/trained_model/model_final", device="cuda")
    # loading the dictionary database
    source_db = Rdict(source_db_path, access_type=AccessType.read_only())
    embedding_db = Rdict(embeddings_db_path)
    docs_iterator = iter(source_db.values())
    new_docs_available = True
    batch_counter = 0
    embedding_index = 0

    while new_docs_available:
        next_docs = get_n_docs(docs_iterator, batch_size)
        new_docs_available = len(next_docs) >= batch_size
        # converting the docInfo list to a list of string for the model to consume
        docs_as_string_list = [doc_info.return_doc_as_text() for doc_info in next_docs]
        embeddings = model.encode(docs_as_string_list)
        batch_counter += 1

        # saving the indices of the embeddings in the final embeddings tensor associated with the index of the doc
        # docs have multiple embeddings for their windows -> many embedding_indices point at one doc_index
        for doc_info in next_docs:
            embedding_db[embedding_index] = doc_info.doc_index
            embedding_index += 1
        embedding_db.flush()

        # each batch is saved alone to counter system failures
        np.save(f"{embeddings_path}/embeddings_{batch_counter}.npy", embeddings)
        print(f"{embedding_index} docs processed")

    return batch_counter


def combine_embeddings(embeddings_path: str, combined_embeddings_name: str, number_of_batches: int):
    """
    Combines the embedding vectors of all batches into a single file.
    The combined embeddings file is saved in the same directory as the separate embeddings file.

    :param embeddings_path: path to the directory where the embeddings are stored
    :param combined_embeddings_name: name of the combined embeddings file without file extension
    :param number_of_batches: number of batches to combine

    :return: None
    """
    concat_array = np.load(f"{embeddings_path}/embeddings_{1}.npy").astype(np.float16)
    counter = 0
    for batch_index in range(2, number_of_batches + 1):
        new_array = np.load(f"{embeddings_path}/embeddings_{batch_index}.npy").astype(np.float16)
        concat_array = np.concatenate((concat_array, new_array), axis=0)

        counter += 1
        if counter >= 50:
            print(f"{batch_index / number_of_batches * 100:.5f}% batches loaded")
            counter = 0

    np.save(f"{embeddings_path}/{combined_embeddings_name}.npy", concat_array)


if __name__ == '__main__':
    batch_number = generate_embeddings("../data/forward_db", "../data/embeddings_db", "../data/embeddings")
    combine_embeddings("../data/embeddings", "combined_embedding_2", batch_number)
