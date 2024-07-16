from naive_retriever import get_result
from rocksdict import Rdict
from query_postprocessing.related_searches import get_related_searches
from query_postprocessing.summarise_text import get_relevant_sentences


db = Rdict("../data/crawl_data")


def n_combined_urls(query: str, n: int, search_factor=5) -> list[str]:
    embedding_results = get_result(query, n * search_factor)

    final_sorted_urls = [result[0] for result in embedding_results]

    return final_sorted_urls[:n]


def n_search_results(query: str, n: int) -> list[tuple[str, str, str]]:
    urls = n_combined_urls(query, n)

    results = []

    for url in urls:
        doc = db.get(url)
        related_searches = get_related_searches([doc])
        important_sentences = get_relevant_sentences(doc, query)
        results.append((url, important_sentences, related_searches))

    return results


if __name__ == '__main__':
    results = n_search_results("University Tübingen", 10)
    print(results)