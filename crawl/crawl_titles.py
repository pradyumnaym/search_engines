
import os
import time
import urllib
import random
import validators
import pickle
import asyncio
import aiohttp
import signal

from multiprocessing import Pool, cpu_count, Process
from dotenv import load_dotenv
from rocksdict import Rdict
from pebble import ProcessPool
from concurrent.futures import TimeoutError

from utils.crawl_parse_utils import  extract_links, extract_text 

load_dotenv()

PARALLEL_REQUESTS = 768          # Number of parallel requests to make
STOP_EVENT = asyncio.Event()      # Flag to stop the crawl

with open('../data/unavailable_urls.pkl', 'rb') as f:
    frontier = set(pickle.load(f))
    if isinstance(next(iter(frontier)), str):
        frontier = set([(url, 0, urllib.parse.urlparse(url).netloc) for url in frontier])

db_titles = Rdict('../data/titles')

# Save the crawl_state file in a subprocess - saves time.
p = None

def write_pickle_file(obj, path):
    print("Saving pickle using subprocess!")
    with open(path, 'wb') as f:
        pickle.dump(obj, f)
    print("Completed saving the state!")

def save_state():
    """
    Save the current crawl state to disk as a pickle file.
    """
    global p

    print("--------------------------------------------------")
    print(f"Saved state at {time.time()}")
    print(f"Number of URLs left: {len(frontier)}")
    print("--------------------------------------------------")

    db_titles.flush()

    if p is None or not p.is_alive():
        # Run this only if the previous subprocess has finished writing to disk.
        p = Process(target=write_pickle_file, args=(frontier, '../data/unavailable_urls.pkl'))
        p.daemon = False
        p.start()


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
        return None, None, None
    
    if url_content == 'timeouterror':
        return 'timeouterror', None, None
    
    try:

        if url.endswith('.pdf'):
            title, text, links  = "PDF File", None, None
        else:
            (text, title), links = extract_text(url_content), None
        
        return text, links, title
            
    except Exception as e:
        print(f"Failed to extract text and links from URL: {e}")
        return None, None, None


async def get_url_content(url, session):
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
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2420.81",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux i686; rv:124.0) Gecko/20100101 Firefox/124.0",
    ]

    headers = {"User-Agent": random.choice(user_agents)}
   

    if any(url.endswith(x) for x in ['.jpg', '.jpeg', '.png', '.gif', '.mp4', '.avi', '.webm']):
        return None

    return_value = None

    try:
        async with session.get(url, timeout=30, headers=headers) as response:
            if url.endswith('.pdf'):
                return_value = await response.read()
            else:
                return_value = await response.text()
    except (aiohttp.ClientError, UnicodeDecodeError, ValueError, LookupError) as e:
        print(f"Failed to fetch {url}: {e}")
    except asyncio.TimeoutError:
        print(f"Failed to fetch {url}: Timeout")
        return_value =  'timeouterror'

    return return_value

def sample_frontier():
    """sample the frontier to get a random sample of URLs to crawl.

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
    
    samples = random.sample(frontier, min(PARALLEL_REQUESTS * 10, len(frontier)))
    visited_items = set()

    # sample 10x the samples, sort the samples by the depth value - need to prioritise the root nodes.
    for url, depth, domain in samples:
        if domain not in domains_frequency:
            domains_frequency[domain] = 0
        if domains_frequency[domain] < 3:
            urls.append(url)
            depths.append(depth)
            domains_frequency[domain] += 1
            visited_items.add((url, depth, domain))

        if len(urls) == PARALLEL_REQUESTS:
            break

    # remove the sampled URLs from the frontier
    frontier.difference_update(visited_items)

    return urls, depths


async def crawl_webpages():
    """crawl the webpages in the frontier.

    This function will run indefinitely and will crawl the webpages in the frontier.

    """
    while len(frontier) > 0:
        
        urls, depths = sample_frontier()

        start_time = time.time()

        connector = aiohttp.TCPConnector(limit=None)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [get_url_content(url, session) for url in urls]
            url_content_raw = await asyncio.gather(*tasks)

        await connector.close()

        url_contents = []

        with ProcessPool() as pool:
            future = pool.map(get_url_text_and_links, zip(urls, url_content_raw), timeout=10)

            iterator = future.result()

            while True:
                try:
                    result = next(iterator)
                    url_contents.append(result)
                except StopIteration:
                    break
                except TimeoutError as error:
                    print("function took longer than %d seconds")
                    url_contents.append((None, None, None))

        assert len(url_contents) == len(urls)

        for url, (url_text, url_links, url_title), depth in zip(urls, url_contents, depths):

            if url_text == 'timeouterror':
                print(f"URL {url} timed out. Adding it to frontier to try again later.")
                frontier.add((url, 0, urllib.parse.urlparse(url).netloc))
                continue

            if url_title is not None:
                db_titles[url] = url_title

        save_state()

        if time.time() - start_time < 10:
            await asyncio.sleep(10 - (time.time() - start_time))

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
