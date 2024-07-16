from engine_kernel.combined_results import n_search_results

'''
Iterface between website and query_postprocessing-engine logic.

Implement a function to get a website-representation for a given query
The representation should conain useful information to display
'''

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