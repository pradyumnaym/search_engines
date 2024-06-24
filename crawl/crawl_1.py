import asyncio
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
from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientError
from dotenv import load_dotenv
from tqdm import tqdm
from bs4 import BeautifulSoup
from rocksdict import Rdict, Options

load_dotenv()

MAX_DEPTH = 7                     # Maximum depth to crawl.
TIME_BETWEEN_REQUESTS = 1.0       # Number of seconds to wait between requests to the same domain
EXPAND_FRONTIER = 0.05            # Probability of expanding the frontier

# Load the frontier URLs
with open('../data/frontier_urls.pkl', 'rb') as f:
    frontier = pickle.load(f)

print(f"Loaded {len(frontier)} URLs from the frontier")

frontier = [(url, MAX_DEPTH) for url in frontier]

current_crawl_state = {
    "frontier": asyncio.Queue(),   # Use asyncio.Queue for thread-safe operation
    "visited": set(),
    "failed": set(),
    "rejected": set(),
    "last_saved": time.time(),
    "to_visit": set()
}

# Initialize the asyncio.Queue with frontier URLs
for url, depth in frontier:
    current_crawl_state['frontier'].put_nowait((url, depth))

del frontier

# Storing the crawl results
db = Rdict('../data/crawl_data')

def save_state():
    """
    Save the current crawl state to disk as a pickle file.
    """
    db.flush()
    state = {}
    for key in current_crawl_state:
        if isinstance(current_crawl_state[key], set) or isinstance(current_crawl_state[key], list):
            state[key] = current_crawl_state[key].copy()
        else:
            state[key] = current_crawl_state[key]

    with open('../data/crawl_state.pkl', 'wb') as f:
        pickle.dump(state, f)

    del state

if os.path.exists('../data/crawl_state.pkl'):
    with open('../data/crawl_state.pkl', 'rb') as f:
        current_crawl_state = pickle.load(f)

# Crawling
exit_event = threading.Event()

# Dictionary to store the last time a domain was accessed
domain_last_accessed = {}

async def check_url_relevance(url_content):
    """Check if the URL is relevant to the topic of the crawl."""
    key_words = ["tübingen", "tuebingen", "boris palmer", "72070", "72072", "72074", "72076", "tubingen", "eberhard karl"]
    for keyword in key_words:
        if keyword in url_content.lower():
            return True
    return False

async def extract_links(current_url, url_content):
    """Extract the links from the HTML content of the URL."""
    soup = BeautifulSoup(url_content, 'html.parser')
    links = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        absolute_url = urllib.parse.urljoin(current_url, href)
        links.append(absolute_url)
    return links

async def extract_text(url_content):
    """Extract the text content from the HTML content of the URL."""
    soup = BeautifulSoup(url_content, 'html.parser')
    return soup.get_text(separator=' ', strip=True)

async def get_url_content(url, session):
    """Get the content of the URL using the requests library."""
    with asyncio.Lock():  # Use asyncio.Lock for thread-safe access
        last_accessed = domain_last_accessed.get(urllib.parse.urlparse(url).netloc)
        if not (last_accessed is None or time.time() - last_accessed >= TIME_BETWEEN_REQUESTS):
            return None
        
        domain_last_accessed[urllib.parse.urlparse(url).netloc] = time.time()

    try:
        async with session.get(url, timeout=30) as response:
            return await response.text()
    except asyncio.TimeoutError:
        return None
    except Exception as e:
        print(f"Failed to crawl {url}: {e}")
        current_crawl_state["failed"].add((url, str(e)))
        return None

async def crawl_webpages(session):
    """Crawl the webpages in the frontier."""
    while not exit_event.is_set():
        try:
            url, depth = await current_crawl_state['frontier'].get()
        except asyncio.QueueEmpty:
            await asyncio.sleep(1)
            continue

        if url in current_crawl_state["visited"] or url in current_crawl_state["rejected"]:
            continue

        url_content = await get_url_content(url, session)

        if url_content is None:
            current_crawl_state["frontier"].put_nowait((url, depth))
            continue

        if not await check_url_relevance(url_content):
            current_crawl_state["rejected"].add(url)
            db[url] = await extract_text(url_content)
            continue

        try:
            db[url] = await extract_text(url_content)
            links = await extract_links(url, url_content)
        except Exception as e:
            print(f"Failed to extract links from {url}: {e}")
            current_crawl_state["failed"].add(url)
            continue

        current_crawl_state["visited"].add(url)

        if depth > 0:
            for link in links:
                if link not in current_crawl_state["visited"] \
                   and link not in current_crawl_state["failed"] \
                   and link not in current_crawl_state["rejected"] \
                   and validators.url(link):

                    if random.random() < EXPAND_FRONTIER:
                        await current_crawl_state["frontier"].put((link, depth - 1))
                    else:
                        current_crawl_state["to_visit"].add((link, depth - 1))

async def save_periodically():
    """Save the crawl state periodically."""
    while not exit_event.is_set():
        await asyncio.sleep(30)
        save_state()
        current_crawl_state["last_saved"] = time.time()
        print("--------------------------------------------------")
        print(f"Saved state at {time.time()}")
        print(f"Visited {len(current_crawl_state['visited'])} URLs")
        print(f"Frontier has {current_crawl_state['frontier'].qsize()} URLs")
        print(f"Failed to crawl {len(current_crawl_state['failed'])} URLs")
        print(f"Rejected {len(current_crawl_state['rejected'])} URLs")
        print("--------------------------------------------------")

async def main():
    async with ClientSession() as session:
        tasks = [
            asyncio.create_task(crawl_webpages(session)),
            asyncio.create_task(save_periodically())
        ]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("KeyboardInterrupt received, shutting down...")
        exit_event.set()