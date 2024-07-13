import chromadb
from rocksdict import Rdict

def get_n_docs(docs, n):
    counter = 0
    pulled_docs = []
    pulled_urls = []

    while counter < n:
        try:
            next_url, next_doc = next(docs)
            if next_doc != "" and next_doc is not None:
                pulled_docs.append(next_doc)
                pulled_urls.append(next_url)
                counter += 1
        except StopIteration:
            break

    return pulled_urls, pulled_docs


def setup_db(db_path: str):
    client = chromadb.PersistentClient(path=db_path)
    collection = client.get_or_create_collection('crawl_data_test')
    return collection


def populate_db(vector_db_path: str, rdict_path: str):
    collection = setup_db(vector_db_path)
    old_db = Rdict(rdict_path)

    # docs are added in batches:
    new_docs_available = True
    batch_size = 1000
    doc_iterator = old_db.items()
    counter = 0
    while new_docs_available:
        n_urls, n_docs = get_n_docs(doc_iterator, batch_size)
        # if the n_docs is no longer batch_size, then the old_db is exhausted
        new_docs_available = len(n_docs) == batch_size
        collection.add(ids=n_urls, documents=n_docs)
        counter += 1
        print(f"{counter}k docs added")



if __name__ == '__main__':
    populate_db("./vector.db", "../data/crawl_data")


