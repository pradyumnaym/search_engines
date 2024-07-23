#from engine_kernel.combined_results import n_search_results
import pickle

'''
Iterface between website and query_postprocessing-engine logic.


Implement a function to get a website-representation for a given query
The representation should contain useful information to display
'''


class SingleResult:

    def __init__(self, url: str, important_sentences: list[str]):
        self.url = url
        self.important_sentences = important_sentences
        self.title = "No Title"


class CompleteResult:
    def __init__(self, related_queries: list[list[list[str]]], results: list[SingleResult]):
        self.related_queries = related_queries
        self.results = results

=======
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



# This is a dummy
def n_search_results(query, n=100) -> CompleteResult:
    with open("../data/falafel_result2.pkl", "rb") as f:
        result = pickle.load(f)
    return CompleteResult(result['related_queries'],
                          [SingleResult(r['url'], r['important_sentences']) for r in result['results']])

#  example representation
example_website = {
    'title': "Some title",
    'url': "http://example.com/",
    'snippet': "Text that descirbes the website"
}

suggested_queries = [
        "Example query 1",
        "Example query 2",
        "Example query 3",
        "Example query 4",
        "Example query 5"
]

# Returns the best matching {n} websites for query q
def get_websites(query: str, count=100):
    # Example websites for testing
    return n_search_results(query, 100)


# Returns examples to debug website
def get_examples(query, count):
    example_results = []
    for i in range(1, count + 1):
        res = {'title': f"Result {i} for query '{query}'",
               'url': f"http://example.com/{i}",
               'snippet': f"This is a snippet for {i}. It provides a brief description of the result. Let's try if it is able to display two lines as well."}
        example_results.append(res)
    return example_results

if __name__ == "__main__":
    with open("../data/falafel_result.pkl", "rb") as f:
        result = pickle.load(f)
    print(result)
=======
