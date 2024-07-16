from naive_retriever import get_result
from rocksdict import Rdict
from query_postprocessing.related_searches import get_related_searches
from query_postprocessing.summarise_text import get_relevant_sentences
from datetime import datetime

db = Rdict("../data/crawl_data")


def n_combined_urls(query: str, n: int, search_factor=5) -> list[str]:
    embedding_results = get_result(query, n * search_factor)

    final_sorted_urls = [result[0] for result in embedding_results]

    return final_sorted_urls[:n]


def n_search_results(query: str, n: int) -> list[tuple[str, str, str]]:
    retriever_start = datetime.now()
    urls = n_combined_urls(query, n)
    retriever_end = datetime.now()

    results = []
    postprocessing_start = datetime.now()
    for url in urls[:10]:
        doc = db.get(url)
        related_searches = get_related_searches([doc])
        important_sentences = get_relevant_sentences(doc, query)
        results.append((url, important_sentences, related_searches))

    for url in urls[10:]:
        results.append((url, "", ""))
    postprocessing_end = datetime.now()

    print("-------------")
    print(f"retrieving time: {retriever_end - retriever_start}")
    print(f"postprocessing time: {postprocessing_end - postprocessing_start}")
    print(f"total time: {postprocessing_end - retriever_start}")
    print("-------------")

    return results


if __name__ == '__main__':
    _ = n_search_results("University Tübingen", 10)

    results = n_search_results("Falafel", 100)
    # print(results)