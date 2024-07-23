from flask import Flask, render_template, request, redirect, url_for
from flask_cors import CORS, cross_origin

# import interface to use search engine backend
from .. import interface
import pickle
import aiohttp
import asyncio
import random

'''
TODO:

- Implement related queries
- Include new dataformat
- Try Iframe stuff
- Maybe Logo


'''

# Run with flask --app website_main run  (--debug)
app = Flask(__name__)
#cors = CORS(app)
#app.config['CORS_HEADERS'] = 'Content-Type'


@app.route("/")
def hello_world():
    return render_template('index.html')

@app.route("/search", methods=['POST'])
def search():
    query = request.form.get('query')
    return redirect(url_for('results', query=query))

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
        async with session.get(url, timeout=15, headers=headers) as response:
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

@app.route("/results")
#@cross_origin()
async def results():
    query = request.args.get('query')
    page = int(request.args.get('page', 1))
    per_page = 10
    total_results = 100

    # results = interface.get_websites(query, count=total_results)
    result = interface.get_websites(query, total_results)
    suggested_queries = result.related_queries[0]
    results = result.results

    # Convert important sentences to one string
    for i in range(len(results)):
        text = ""
        for s in results[i].important_sentences:
            text += s + "\n"
        
        if text == "":
            text = "Sadly there couldn't be found any interesting sentences to display here, if there where any, you could read them now. Instead you just wasted your time reading a lot of nonsens that is just so longto check if hat could cause any problems with formatting."
    
        results[i].important_sentences = text
        
    start = (page - 1) * per_page
    end = start + per_page
    paginated_results = results[start:end]


    urls = [x.url for x in paginated_results]

    connector = aiohttp.TCPConnector(limit=None)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [get_url_content(url, session) for url in urls]
        url_content_raw = await asyncio.gather(*tasks)

    await connector.close()

    print("Gathered all URL contents!")
    print(url_content_raw[0])

    total_pages = total_results // per_page
    
    return render_template('results.html', query=query, results=paginated_results, page=page, total_pages=total_pages, suggested_queries=suggested_queries, url_contents=url_content_raw)