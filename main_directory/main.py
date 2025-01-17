from engine_kernel.combined_results import n_combined_urls
from query_postprocessing.related_searches import get_related_searches
from query_postprocessing.summarise_text import get_relevant_sentences
from rocksdict import Rdict, AccessType
from datetime import datetime
from interface import *


forward_db = Rdict("../data/runtime_data/forward_db", access_type=AccessType.read_only())
backward_db = Rdict("../data/runtime_data/backward_db", access_type=AccessType.read_only())
title_db = Rdict("../data/runtime_data/titles", access_type=AccessType.read_only())


# Returns the best matching {n} websites for query q
def n_search_results(query: str, n: int, search_factor=5) -> CompleteResult:
    retriever_start = datetime.now()
    simple_results = n_combined_urls(query, n * search_factor)
    retriever_end = datetime.now()

    postprocessing_start = datetime.now()
    related_searches = get_related_searches([query])

    results = []
    for doc_index, score in simple_results:
        url = backward_db[doc_index]
        doc_info: DocInfo = forward_db[url]
        try:
            title = title_db[url]
        except KeyError:
            title = "No title found"

        important_sentences = get_relevant_sentences(doc_info.return_doc_as_text(), query)
        results.append(SingleResult(url, score, title, important_sentences))

    answers = CompleteResult(related_searches, results[:n])
    postprocessing_end = datetime.now()

    print("-------------")
    print(f"retrieving time: {retriever_end - retriever_start}")
    print(f"postprocessing time: {postprocessing_end - postprocessing_start}")
    print(f"total time: {postprocessing_end - retriever_start}")
    print("-------------")

    return answers


def interactive_exam_file_creation():
    with open("exam_output.txt", "w") as file:
        query = input("Please enter a query: ")
        query_num = 1
        while query != "q":
            result = n_search_results(query, 100, search_factor=1)
            result.output_str(file, query_num=query_num)
            query_num += 1
            query = input("Please enter next query: ")


if __name__ == '__main__':
    interactive_exam_file_creation()
