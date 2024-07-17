from engine_kernel.combined_results import n_search_results

'''
Iterface between website and query_postprocessing-engine logic.

Implement a function to get a website-representation for a given query
The representation should conain useful information to display
'''


class SingleResult:
    def __init__(self, url: str, important_sentences: list[str]):
        self.url = url
        self.important_sentences = important_sentences


class CompleteResult:
    def __init__(self, related_queries: list[list[list[str]]], results: list[SingleResult]):
        self.related_queries = related_queries
        self.results = results


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




#  example representation
example_website = {
    'title': "Some title",
    'url': "http://example.com/",
    'snippet': "Text that descirbes the website"
}

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