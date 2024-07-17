from naive_retriever import get_result
from rocksdict import Rdict
from query_postprocessing.related_searches import get_related_searches
from query_postprocessing.summarise_text import get_relevant_sentences
from datetime import datetime
from interface import SingleResult, CompleteResult

db = Rdict("../data/crawl_data")


def combine_two_scores(score_dict1: dict[str, float], score_dict2: dict[str, float], alpha=0.5) -> dict[str, float]:
    result: dict[str, float] = {}

    for key, score1 in score_dict1.values():
        if key in score_dict2:
            result[key] = alpha * score1 + (1 - alpha) * score_dict2[key]
            del score_dict2[key]
        else:
            result[key] = score1

    for key, score2 in score_dict2.values():
        result[key] = score2

    return result


def n_combined_urls(query: str, n: int, search_factor=5) -> list[str]:
    embedding_results = get_result(query, n * search_factor)

    final_sorted_urls = [result[0] for result in embedding_results]

    return final_sorted_urls[:n]


def n_search_results(query: str, n: int) -> CompleteResult:
    retriever_start = datetime.now()
    urls = n_combined_urls(query, n)
    retriever_end = datetime.now()

    related_searches = get_related_searches([query])

    results = []
    postprocessing_start = datetime.now()
    for url in urls:
        doc = db.get(url)
        important_sentences = get_relevant_sentences(doc, query)
        results.append(SingleResult(url, important_sentences))

    answers = CompleteResult(related_searches, results)
    postprocessing_end = datetime.now()

    print("-------------")
    print(f"retrieving time: {retriever_end - retriever_start}")
    print(f"postprocessing time: {postprocessing_end - postprocessing_start}")
    print(f"total time: {postprocessing_end - retriever_start}")
    print("-------------")

    return answers


if __name__ == '__main__':
    _ = n_search_results("University Tübingen", 10)

    _ = n_search_results("Falafel", 100)
    # print(results)
