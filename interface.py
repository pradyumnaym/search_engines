from engine_kernel.combined_results import n_combined_urls
from query_postprocessing.related_searches import get_related_searches
from query_postprocessing.summarise_text import get_relevant_sentences
from rocksdict import Rdict
from datetime import datetime

'''
Iterface between website and query_postprocessing-engine logic.

Implement a function to get a website-representation for a given query
The representation should contain useful information to display
'''


forward_db = Rdict("data/runtime_data/forward_db")
backward_db = Rdict("data/runtime_data/backward_db")


class SingleResult:
    def __init__(self, url: str, score, important_sentences: list[str]):
        self.url = url
        self.score = score
        self.important_sentences = important_sentences


class CompleteResult:
    def __init__(self, related_queries: list[list[str]], results: list[SingleResult]):
        self.related_queries = related_queries
        self.results = results

    def print_complete_results(self):
        for result in self.results:
            print(f"--- {result.url}, with score {result.score}")
            print(result.important_sentences)
            print("---------")

    def output_str(self, output_file, query_num=0):
        result_rank = 1
        for result in self.results:
            output_file.write(f"{query_num}\t{result_rank}\t{result.url}\t{result.score}\n")
            result_rank += 1



class DocInfo:
    def __init__(self, doc_index: int, word_list: list[str]):
        self.doc_index = doc_index
        self.word_list = word_list

    def return_doc_as_text(self) -> str:
        doc_string = ""
        for word in self.word_list:
            doc_string += word + " "

        doc_string = doc_string[:-1]  # removing the last space
        return doc_string


# Returns the best matching {n} websites for query q
def n_search_results(query: str, n: int) -> CompleteResult:
    retriever_start = datetime.now()
    simple_results = n_combined_urls(query, n)
    retriever_end = datetime.now()

    postprocessing_start = datetime.now()
    related_searches = get_related_searches([query])

    results = []
    for doc_index, score in simple_results:
        url = backward_db.get(doc_index)
        doc_info = forward_db.get(url)

        important_sentences = get_relevant_sentences(doc_info.return_doc_as_text(), query)
        results.append(SingleResult(url, score, important_sentences))

    answers = CompleteResult(related_searches, results)
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
        while query is not "q":
            result = n_search_results(query, 100)
            result.output_str(file, query_num=query_num)
            query_num += 1
            query = input("Please enter next query: ")


if __name__ == '__main__':
    import pickle
    #  = n_search_results("University Tübingen", 10)

    complete_result = n_search_results("Falafel", 100)
    complete_result.print_complete_results()
    with open("./falafel_result.pkl", "rb") as outfile:
        pickle.dump(complete_result, outfile)