'''
This file contains functions to calculate the bm25-value of a query, document pair
As well as get the n best matching documents for a query
'''

from preprocess import get_size
import preprocess
import pickle
from rocksdict import Rdict
import time
import os
import sys
import pickle
import numpy as np
# Add the parent directory to the system path
parent_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))
sys.path.insert(0, parent_dir)
import interface


# variables needed
inverted_index = {}
backward_db = {}
doc_count = 0
avg_doc_len = 0
doc_len = {}

index_path = "../data/bm25/inverted_index50k"
backward_path = "../data/backward_db"
doc_len_path = "../data/bm25/doc_len.pkl"


inverted_index = Rdict(index_path)
backward_db = Rdict(backward_path)
with open("../data/bm25/doc_len.pkl", 'rb') as f:
    doc_len = pickle.load(f)

doc_count = len(doc_len.keys())
counter = 0
for l in doc_len.values():
    counter += l
    
avg_doc_len = counter / doc_count


def get_preselection(query: list) -> tuple[set, dict]:
    '''
    Preselect websites that contain at least one word of teh query for better perfomrance
    It also returns a smaller inverted index that contains all necessary information
    Instead of urls, indices are used

    :param query: List of the preprocessed query words

    :return: ({urlA, urlB, ...}, {"word": {urlA: tf, urlB: tf}}, "wordB": {urlA: tf}, ...)
    '''
    websites = set()
    small_index = {}
    for word in query:
        try:
            elements = inverted_index[word] # (idx, tf) pairs
        except KeyError:
            elements = []

        temp = {}
        for idx, tf in elements:
            websites.add(idx)
            temp[idx] = tf
        
        small_index[word] = temp

    return websites, small_index
    

def calc_bm25(index, query, document, k=1.5, b=0.75):
    '''
    Calculates the bm25-value for a document given a query
    Formula as discussed in the lecture

    :param index: inverted index to use
    :param query: List of query words
    :param document: Document to calculate the bm25 for
    :param k: k value
    :param b: b value

    :return: Float, bm-25 score
    '''
    document_len = doc_len[document]
    value = 0

    for word in query:
        try:
            tf = index[word][document]
            idf = np.log(doc_count / len(index[word]))
            bm25_step = idf * ((tf * (k + 1)) / (tf + (k * (1 - b + (b * (document_len / avg_doc_len))))))
            value += bm25_step
        except KeyError:
            pass

    return value


def _insert(matches: list, max_size: int, match):
    '''
    Insert a idx, value pair into a sorted list with a fixed size

    :param matches: sorted list where items should be inserted into
    :param max_size: maximum size of the list
    :param match: dict holding value to store. needs to have the key 'score' to be sorted after that attribute

    :return: Sorted list with match inserted
    '''
    score = match['score']
    output = matches
    idx = 0
    for m in matches:
        # if this is not the place for match, increment i
        if m['score'] > score:
            idx += 1
        else:
            break

    if idx < max_size:
        output = matches[:idx] + [match] + matches[idx:]

    if len(output) > max_size:
        output = output[:max_size]

    return output


def retrieve(query: str, n=100) -> list:
    '''
    Get the n best matches for a query using he bm25 model

    :param query: Query string
    :param n: Number of results to get (default is 100)

    :return: List of urls and scores
    '''
    query = preprocess.preprocess(query)
    print(query)
    websites, small_index = get_preselection(query)
    matches = []

    for url_idx in list(websites):
        score = calc_bm25(small_index, query, url_idx)
        matches = _insert(matches, n, {'url': backward_db[url_idx], 'score': score})

    return matches


def close():
    '''
    Close databases
    '''
    inverted_index.close()
    backward_db.close()


if __name__ == '__main__':

    start_ts = time.time()
    result = retrieve("Uni Tübingen informatik Fakultät Search engines Lecture computer science")
    duration_s = time.time() - start_ts
    print(duration_s)
    print(result)

    close()