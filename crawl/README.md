## Crawler 


### Setup instructions:
Please install all requirements using `pip install -r requirements.txt`.

In case of install errors for fasttext on some distributions of Linux, please run the following two commands:

1. `apt-get install build-essential -y` might be required for some linux distributions.
2. `sudo apt-get install python3-dev`

### Files
We start gathering information about places in Tübingen using OpenStreetMaps. We then use this data to generate some keywords using the LLM `Mistral-7B-Instruct-v0.3` . We then use these queries to generate a seed set of URLs for the frontier using an existing search engine. It is done in the following steps:

1. `frontier-setup.ipynb`: We query OpenStreetMaps with a bounding box around Tübingen, for nodes of interest, such as restaurants, hospitals, etc. Here, some nodes already have information such as name, url. For nodes without such identifiable information, we use an LLM to generate search keywords. 

2. `create_frontier_urls.ipynb`: We use the extracted keywords data to create a frontier of URLs, using the Bing Search API.

3. `crawl_webpages.(py | ipynb)`: We start with the frontier URLs, and crawl all relevant webpages. Initially, we determin relevance by simply looking for keywords such as 'Tübingen'. Once we have enough data, we can train a classifier to determine relevance. `crawl_webpages.py` has a thread-based implementation that gets bottlenecked due to the synchronous requests.get() call. We then implement `crawl_webpages.py` with async requests and multiprocessing.

4. `filter_english_data.ipynb`: A helper notebook to remove all German documents, and to edit the crawl state to only keep English documents.


