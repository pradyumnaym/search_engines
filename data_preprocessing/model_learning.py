from sentence_transformers.losses.ContrastiveTensionLoss import ContrastiveTensionLoss
from sentence_transformers.losses import ContrastiveTensionDataLoader
from sentence_transformers import SentenceTransformer
from rocksdict import Rdict, AccessType
import pickle


def save_dataset_parts(batch_size: int):
    """
    A helping function to slice the dataset into parts so that they can be loaded directly into RAM later.

    :param batch_size: The size of the dataset to slice.
    """
    forward_db = Rdict("../data/forward_db", access_type=AccessType.read_only())

    docs = []
    doc_counter = 0
    batch_counter = 0

    for doc_info in forward_db.values():
        # since the model has a limited input size the documents are cuts after 700 words
        if len(doc_info.word_list) > 700:
            doc_info.word_list = doc_info.word_list[:700]

        docs.append(doc_info.return_doc_as_text())
        doc_counter += 1

        if doc_counter >= batch_size:
            # if one batch is full it is saved and a new batch is started
            with open(f"../data/dataset_windows/dataset_window_{batch_counter}.pickle", 'wb') as f:
                pickle.dump(docs, f, pickle.HIGHEST_PROTOCOL)

            print(f"Batch {batch_counter} saved")
            batch_counter += 1
            doc_counter = 0
            docs = []


def train_model(batch_size: int, epochs: int):
    """
    function to train the model on the prepared docs parts

    :param batch_size: The size of the training batch. Depending on the RAM
    :param epochs: The number of epochs to train the model
    """
    model = SentenceTransformer("all-mpnet-base-v2", device="cuda")
    loss_func = ContrastiveTensionLoss(model)

    for dataset_index in range(21):
        with open(f"../data/dataset_windows/dataset_window_{dataset_index}.pickle", 'rb') as f:
            doc_list = pickle.load(f)
            dataloader = ContrastiveTensionDataLoader(doc_list, batch_size=batch_size, pos_neg_ratio=3)
            model.fit([(dataloader, loss_func)], epochs=epochs)

    model.save_pretrained(f"../data/trained_model/model_final")


if __name__ == '__main__':
    train_model(9, 1)


