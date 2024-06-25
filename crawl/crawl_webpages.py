
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
import fitz
import asyncio
import aiohttp
from ftlangdetect import detect

from requests_ip_rotator import EXTRA_REGIONS, ApiGateway
from multiprocessing import Pool, cpu_count
from dotenv import load_dotenv
from tqdm import tqdm
from bs4 import BeautifulSoup

from rocksdict import Rdict, Options

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

MAX_DEPTH = 7                     # Maximum depth to crawl.
TIME_BETWEEN_REQUESTS = 1.0       # Number of seconds to wait between requests to the same domain
EXPAND_FRONTIER = 0.5             # Probability of expanding the frontier
PARALLEL_REQUESTS = 2048          # Number of parallel requests to make
STOP_EVENT = asyncio.Event()      # Flag to stop the crawl

# load the frontier URLs

with open('../data/frontier_urls.pkl', 'rb') as f:
    frontier = pickle.load(f)

print(f"Loaded {len(frontier)} URLs from the frontier")

# each frontier entry has the url, the depth, and the domain of the URL
frontier = [(url, MAX_DEPTH, urllib.parse.urlparse(url).netloc) for url in frontier if validators.url(url)]

current_crawl_state = {
    "frontier": frontier,           # list of URLs to be crawled
    "visited": set(),               # list of URLs that have been crawled (we only store the URL, not the content)
    "failed": set(),                   # list of URLs that have failed to be crawled.
    "rejected": set(),                 # list of URLs that were rejected based on key word relevance
    "last_saved": time.time(),       # timestamp of the last save
    "to_visit": set(),               # list of URLs that are yet to be crawled
    "all_discovered_urls": set(
            [
                item[0] for item in frontier \
                if not any(item[0].endswith(x) for x in ['.jpg', '.jpeg', '.png', '.gif', '.mp4', '.avi', '.webm'])
            ]
            )    # list of all URLs that have been processed
}

del frontier

if os.path.exists('../data/crawl_state.pkl'):
    with open('../data/crawl_state.pkl', 'rb') as f:
        current_crawl_state = pickle.load(f)

# #### Storing the crawl results
# 
# We store the results of the crawl in a `rocksDB` instance, which is a simple key-value store. We use the `rocksdict` library that provides a nice, dict-like interface to the key-value store. This takes care of caching data on memory, and flushing the results to the database as required.

# open the dictionary file
db = Rdict('../data/crawl_data')

def save_state():
    """
    Save the current crawl state to disk as a pickle file.
    """
    current_crawl_state["last_saved"] = time.time()

    print("--------------------------------------------------")
    print(f"Saved state at {time.time()}")
    print(f"Visited {len(current_crawl_state['visited'])} URLs")
    print(f"Frontier has {len(current_crawl_state['frontier'])} URLs")
    print(f"Failed to crawl {len(current_crawl_state['failed'])} URLs")
    print(f"Rejected {len(current_crawl_state['rejected'])} URLs")
    print("--------------------------------------------------")

    db.flush()
    with open('../data/crawl_state.pkl', 'wb') as f:
        pickle.dump(current_crawl_state, f)

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

def extract_links(current_url, url_content, max_links=100):
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

    if len(links) > max_links:
        links = random.sample(links, max_links)
    
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

async def get_url_content(url):
    """fetch the content of the URL using aiohttp.
    We use aiohttp to fetch the content of the URL asynchronously

    Arguments
    ---------
    url : str
        the URL to fetch

    Returns
    -------
    str
        the content of the URL
    """

    connector = aiohttp.TCPConnector(limit=None)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            async with session.get(url, timeout=30) as response:
                if url.endswith('.pdf'):
                    return await response.read()
                return await response.text()
        except (aiohttp.ClientError, UnicodeDecodeError, ValueError, LookupError) as e:
            print(f"Failed to fetch {url}: {e}")
            return None
        except asyncio.TimeoutError:
            print(f"Failed to fetch {url}: Timeout")
            return None

def sample_frontier():
    """sample the frontier to get a random sample of URLs to crawl.
    We sample the frontier to get a random sample of URLs to crawl.

    Returns
    -------
    list
        list of URLs to crawl
    list
        list of depths of the URLs

    """

    urls = []
    depths = []
    domains_frequency = {}
    for url, depth, domain in random.sample(current_crawl_state['frontier'], PARALLEL_REQUESTS * 10):
        if domain not in domains_frequency:
            domains_frequency[domain] = 0
        if domains_frequency[domain] < 3:
            urls.append(url)
            depths.append(depth)
            domains_frequency[domain] += 1

        if len(urls) == PARALLEL_REQUESTS:
            break

    return urls, depths

def get_url_text_and_links(args):
    """get the text content and links from the URL.
    We extract the text content and links from the URL.

    Arguments
    ---------
    args : tuple
        tuple containing the URL and the content of the URL

    Returns
    -------
    str
        the text extracted from the content
    list
        list of URLs extracted from the content

    """

    url, url_content = args

    if url_content is None:
        return None, None
    
    try:

        if isinstance(url_content, bytes):
            pdf_document = fitz.open(stream=url_content, filetype="pdf")
            text = ""
            for page_num in range(len(pdf_document)):
                page = pdf_document[page_num]
                text += page.get_text()

            pdf_document.close()
            links = []
        
        else:
            text, links = extract_text(url_content), extract_links(url, url_content)

        if detect(text.replace('\n', ' '), low_memory=False)['lang'] != 'en':
            return None, []
        
        return text, links
            
    except Exception as e:
        print(f"Failed to extract text and links from URL: {e}")
        return None, None

async def crawl_webpages():
    """crawl the webpages in the frontier.

    This function will run indefinitely and will crawl the webpages in the frontier.

    """
    while len(current_crawl_state["frontier"]) > 0:
        
        urls, depths = sample_frontier()
        tasks = [get_url_content(url) for url in urls]
        url_contents = await asyncio.gather(*tasks)

        with Pool(cpu_count()) as p:
            url_contents = p.map(get_url_text_and_links, zip(urls, url_contents))

        all_new_links = set()

        for url, (url_text, url_links), depth in zip(urls, url_contents, depths):

            current_crawl_state['all_discovered_urls'].add(url)

            if url_text is None:
                
                #language does not match.
                if url_links is not None:
                    db[url] = url_text
                    current_crawl_state['rejected'].add(url)

                # failed to fetch the URL content
                current_crawl_state["failed"].add(url)
                continue

            # check if the URL is relevant
            if not check_url_relevance(url_text):
                current_crawl_state["rejected"].add(url)
                db[url] = url_text
                continue
            
            # save the text content to the dictionary
            db[url] = url_text

            current_crawl_state["visited"].add(url)

            if depth > 0:
                all_new_links.update(url_links)

        # now process all the new links - add them to the frontier or to_visit, after checking if they are already discovered
        all_new_links = all_new_links - current_crawl_state['all_discovered_urls']

        for link in all_new_links:
            # check if it is a valid URL, and not an image or a video
            if validators.url(link) and not any(url.endswith(x) for x in ['.jpg', '.jpeg', '.png', '.gif', '.mp4', '.avi', '.webm']):
                current_crawl_state["frontier"].append((link, MAX_DEPTH, urllib.parse.urlparse(link).netloc))
                # add the link to the frontier with a probability to avoid the frontier becoming too large
                if random.random() < EXPAND_FRONTIER:
                    current_crawl_state["frontier"].append((link, depth-1, urllib.parse.urlparse(link).netloc))
                else:
                    current_crawl_state["to_visit"].add((link, depth-1, urllib.parse.urlparse(link).netloc))
          
        save_state()
        if STOP_EVENT.is_set():
            break

def signal_handler(sig, frame):
    print("KeyboardInterrupt received, shutting down...")
    STOP_EVENT.set()

# # Setup signal handling
loop = asyncio.get_event_loop()

# Setup signal handling
loop.add_signal_handler(signal.SIGINT, lambda: signal_handler(signal.SIGINT, None))

try:
    loop.run_until_complete(crawl_webpages())
finally:
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
