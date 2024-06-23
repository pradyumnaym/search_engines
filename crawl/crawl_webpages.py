
import os
import gc
import json
import time
import urllib
import requests
import random
import validators
import threading
import pickle
import signal
from requests_ip_rotator import EXTRA_REGIONS, ApiGateway

from dotenv import load_dotenv
from tqdm import tqdm
from bs4 import BeautifulSoup

from rocksdict import Rdict, Options

# TODOs:
# change the domain waiting -> switch to a different url instead of waiting

load_dotenv()
# #### Crawler design
# 
# We now have a frontier of URLs. We now need to carefully crawl all linked webpages while ensuring to only index relevant webpages. 
# 
# **Design of the crawler**: We can see that the crawling process is _network bound_, i.e, the bottleneck is the network latency and the server rate limit. So despite the [GIL](https://wiki.python.org/moin/GlobalInterpreterLock), we can safely use multiple threads within the same python process without any performance issues. For the purposes of small-scale crawling, we do not need multiple processes. This has some advasntages:
# 
# 1. We do not need complex synchronisation mechanisms between different threads. The Python GIL ensures that all shared accesses are safe. (Because threads are _concurrent_, and not _parallel_).
# 2. We can simply use a python list as our shared data structure for the frontier! (List operations  such as append and pop are thread-safe by [default](https://web.archive.org/web/20201108091210/http://effbot.org/pyfaq/what-kinds-of-global-value-mutation-are-thread-safe.htm))
# 
# Enqueuing and dequeueing as simply done through list append and list pop operations. 

MAX_DEPTH = 8                    # Maximum depth to crawl.
TIME_BETWEEN_REQUESTS = 1.25        # Number of seconds to wait between requests to the same domain

# load the frontier URLs

with open('../data/frontier_urls.pkl', 'rb') as f:
    frontier = pickle.load(f)

print(f"Loaded {len(frontier)} URLs from the frontier")

frontier = [(url, MAX_DEPTH) for url in frontier]

current_crawl_state = {
    "frontier": frontier,           # list of URLs to be crawled
    "visited": set(),               # list of URLs that have been crawled (we only store the URL, not the content)
    "failed": set(),                   # list of URLs that have failed to be crawled.
    "rejected": set(),                 # list of URLs that were rejected based on key word relevance
    "last_saved": time.time()       # timestamp of the last save
}

del frontier

# #### Storing the crawl results
# 
# We store the results of the crawl in a `rocksDB` instance, which is a simple key-value store. We use the `rocksdict` library that provides a nice, dict-like interface to the key-value store. This takes care of caching data on memory, and flushing the results to the database as required.

# open the dictionary file
db = Rdict('../data/crawl_data')

def save_state():
    """
    Save the current crawl state to disk as a pickle file.
    """
    db.flush()
    with open('../data/crawl_state.pkl', 'wb') as f:
        pickle.dump(current_crawl_state, f)

if os.path.exists('../data/crawl_state.pkl'):
    with open('../data/crawl_state.pkl', 'rb') as f:
        current_crawl_state = pickle.load(f)

# #### Crawling
# 
# We use a multi-threaded crawler for the reasons discussed above. Most of the operations we use below are thread-safe except for the following: 
# 
# * The `pop` operation is itself atomic, but we _read_ the length of the list to pop a random URL. This can cause issues and is not thread safe. So we use a mutex lock to ensure only one thread pops at once.
# * Similar to the above, `dict` operations are thread safe, but we need to ensure only one thread reads the dict at a time. So we use another lock.
# * To ensure only one thread saves the state at a time, we use another lock.
# 
# The crawling process is straightforward:
# 1. Pop a URL from the frontier
# 2. Check if enough time has passed since the previous request to the domain.
# 3. If yes, retrieve the contents of the URL
# 4. Extract contents and links from the URL.
# 5. Append the links to the frontier, and save the contents of the URL.


frontier_lock = threading.Lock()    # lock to access the frontier - needed because we read the length - not atomic!
dict_read_lock = threading.Lock()   # lock to read from the dictionary - needed because reads are not atomic
exit_event = threading.Event()      # Event to signal an exit to all threads.

# dictionary to store the last time a domain was accessed
# we can use this to avoid hitting the same domain too frequently across different crawlers
domain_last_accessed = {}

def check_url_relevance(url_content):
    """check if the URL is relevant to the topic of the crawl
    we can use a simple heuristic to check if the URL contains the keyword
    or we can use a more sophisticated method to check the content of the page
    to see if it is relevant
    
    Arguments
    ---------
    url_content : str
        the URL to check

    Returns
    -------
    bool
        True if the URL is relevant, False otherwise

    """

    key_words = ["tübingen", "tuebingen", "boris palmer", "72070", "72072", "72074", "72076", "tubingen", "eberhard karl"]

    for keyword in key_words:
        if keyword in url_content.lower():
            return True

    return False

def extract_links(current_url, url_content):
    """extract the links from the HTML content of the URL.
    We can use BeautifulSoup to extract the links from the HTML content
    We need to take care of relative URLs and convert them to absolute URLs
    
    Arguments
    ---------
    current_url : str
        the URL of the page from which the content was extracted
    url_content : str
        the HTML content of the URL

    Returns
    -------
    list
        list of URLs extracted from the content

    """
    soup = BeautifulSoup(url_content, 'html.parser')
    links = []
    
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        absolute_url = urllib.parse.urljoin(current_url, href)
        links.append(absolute_url)
    
    return links

def extract_text(url_content):
    """extract the text content from the HTML content of the URL.
    We can use BeautifulSoup to extract the text from the HTML content
    
    Arguments
    ---------
    url_content : str
        the HTML content of the URL

    Returns
    -------
    str
        the text extracted from the content

    """
    soup = BeautifulSoup(url_content, 'html.parser')
    return soup.get_text(separator=' ', strip=True)

def get_url_content(url):
    """get the content of the URL using the requests library.
    We need to check the previous access time of the domain before we can access it
    
    Arguments
    ---------
    url : str
        the URL to crawl

    Returns
    -------
    str
        the content of the URL

    """
    # We need to wait for two things: 
    # 1. We need to wait for at least 2 seconds before we crawl the same domain again
    # 2. We need to wait for the lock to be released before we can check the domain_last_accessed

    with dict_read_lock:
        last_accessed = domain_last_accessed.get(urllib.parse.urlparse(url).netloc)

        if not (last_accessed is None or time.time() - last_accessed >= TIME_BETWEEN_REQUESTS):
            return None
        
        # we can access the domain. We need to update the last accessed time
        domain_last_accessed[urllib.parse.urlparse(url).netloc] = time.time()

    # get the content of the URL
    return requests.get(url, timeout=30).text

def crawl_webpages():
    """crawl the webpages in the frontier.

    This function will run indefinitely and will crawl the webpages in the frontier.

    """

    while not exit_event.is_set():
        
        with frontier_lock:
            # we pop a random URL - it is important to randomize the order of the URLs 
            # to avoid multiple crawlers hitting the same website at the same time
                url, depth = current_crawl_state['frontier'].pop(random.randrange(len(current_crawl_state['frontier'])))

        if url in current_crawl_state["visited"] or url in current_crawl_state["rejected"]:
            continue
        
        try:
            url_content = get_url_content(url)

            if url_content is None:
                current_crawl_state["frontier"].append((url, depth))
                continue

        except Exception as e:
            print(f"Failed to crawl {url}: {e}")
            current_crawl_state["failed"].add((url, str(e)))
            continue

        # check if the URL is relevant
        if not check_url_relevance(url_content):
            current_crawl_state["rejected"].add(url)
            db[url] = extract_text(url_content)
            continue

        current_crawl_state["visited"].add(url)
        # save the text content to the dictionary
        db[url] = extract_text(url_content)
        # extract the links from the content
        links = extract_links(url, url_content)

        if depth > 0:
            # add the links to the frontier
            for link in links:
                if link not in current_crawl_state["visited"] and link not in current_crawl_state["failed"]:
                    current_crawl_state['frontier'].append((link, depth-1))


def signal_handler(sig, frame):
    print("KeyboardInterrupt received, shutting down...")
    exit_event.set()  # Signal all threads to exit

# Setup signal handling
signal.signal(signal.SIGINT, signal_handler)

# start the crawler threads

threads = [threading.Thread(target=crawl_webpages) for _ in range(8)]

for thread in threads:
    thread.start()

while True:
    if all([not thread.is_alive() for thread in threads]):
        break
    time.sleep(240)
    save_state()
    current_crawl_state["last_saved"] = time.time()
    gc.collect()

    print("--------------------------------------------------")
    print(f"Saved state at {time.time()}")
    print(f"Visited {len(current_crawl_state['visited'])} URLs")
    print(f"Frontier has {len(current_crawl_state['frontier'])} URLs")
    print(f"Failed to crawl {len(current_crawl_state['failed'])} URLs")
    print(f"Rejected {len(current_crawl_state['rejected'])} URLs")
    print("--------------------------------------------------")


# save the final state
save_state()  

# requests.get('https://www.wein-bauer.de/Weine/')