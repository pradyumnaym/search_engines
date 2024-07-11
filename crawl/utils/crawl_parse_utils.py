import signal
import random
import urllib
import fitz
import time

from bs4 import BeautifulSoup
from ftlangdetect import detect

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException

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

    if soup.title is None:
        return soup.get_text(separator=' ', strip=True), None

    return soup.get_text(separator=' ', strip=True), soup.title.text


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
            pdf_document = fitz.open(stream=url_content, filetype="pdf")
            text = ""
            for page_num in range(len(pdf_document)):
                page = pdf_document[page_num]
                text += page.get_text()

            pdf_document.close()
            links = []
            title = None
        
        else:
            (text, title), links = extract_text(url_content), extract_links(url, url_content)
            
        if detect(text.replace('\n', ' '), low_memory=True)['lang'] != 'en':
            return None, [], None
        
        return text, links, title
            
    except Exception as e:
        print(f"Failed to extract text and links from URL: {e}")
        return None, None, None
