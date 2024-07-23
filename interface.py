#from engine_kernel.combined_results import n_search_results
import pickle

'''
Iterface between website and query_postprocessing-engine logic.


Implement a function to get a website-representation for a given query
The representation should contain useful information to display
'''


class SingleResult:
    def __init__(self, url: str, score, important_sentences: list[str]):
        self.url = url
        self.important_sentences = important_sentences
        self.score = score
        self.title = "No Title"


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

