import aiohttp
import asyncio
import json
import pickle
import time
import validators

with open('../../data/frontier_urls.pkl', 'rb') as f:
    frontier = pickle.load(f)

urls = list(frontier)[:1000]

urls = [url for url in urls if validators.url(url)]

count = 0

async def fetch(url):
    global count
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=30) as response:
                return await response.text()
        except aiohttp.ClientError as e:
            count += 1
            print(f"Failed to fetch {url}: {e}")
            return None
        except asyncio.TimeoutError:
            count += 1
            print(f"Failed to fetch {url}: Timeout")
            return None
        except UnicodeDecodeError as e:
            count += 1
            print(f"Failed to fetch {url}: {e}")
            return None
    
async def main():
        tasks = [fetch(url) for url in urls]
        htmls = await asyncio.gather(*tasks)
        return htmls
    
start_time = time.time()
htmls = asyncio.run(main())
end_time = time.time()
print(f"Time taken to fetch {len(urls)} URLs: {end_time - start_time:.2f} seconds")
print(f"Failed to fetch {count} URLs")
print()