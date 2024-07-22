import pickle
import os
import time
from rocksdict import Rdict


punc = '''|!()-[]{};:'",<>./?@#$%^&*_~\\“”’‘'''
# load basic stopword-file
with open('../data/stopwords.txt', 'r') as f:
    stopwords = f.read().splitlines()


def remove_punctuation(word: str) -> str:
    '''
    Removes punctuation from a string based on all punctuation characters defined in this file.

    :param word: The word to remove punctuation from

    :return: word without punctuation
    '''
    return ''.join([char for char in word if char not in punc])


numbers = "0123456789"
def is_number(word: str) -> bool:
    '''
    Detects if a word contains more than 3 digits
    We want to kick out words that have too much numbers because of performance issues

    :param word: The word to check if it contains digits

    :return: bool is the word a number?
    '''
    num_count = 0
    idx = 0
    while num_count <= 3 and idx < len(word):
        if word[idx] in numbers:
            num_count += 1
        idx += 1
    return num_count > 3


ascii_chars = '''!"#$%&()*+'-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~€‚ƒ„…†‡ˆ‰Š‹ŒŽ“”•–—˜™š›œžŸ¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ'''
def is_sus(word: str) -> bool:
    '''
    Detects if a word contains non-ascii characters
    We want to kick out words that contains characters which no one will ever search for

    :param word: The word to check

    :return: bool does the word contain non-ascii characters
    '''
    return any([c not in ascii_chars for c in word])


def preprocess(text) -> list:
    '''
    Preprocesses a given text based on basic principles defined in this file.
    Punctuation is removed based on a simple punctuation string.
    Stop words are removed based on a simple stopword file.
    All words are lowercased.
    Text is transformed into a list of words.
    Whitespaces are removed.
    words containing non-ascii characters are removed
    words containing too many numbers are removed

    :param text: The text that should be preprcessed. Either a lit of words or a string.

    :return: List of preprocessed words.
    '''
    if isinstance(text, str):
        words = text.split()
    else:
        words = text
    output = []
    for word in words:
        w = word.lower()
        w = remove_punctuation(w)
        illegal = is_number(w) or is_sus(w) or (w in stopwords)
        if w != "" and not illegal:
            output.append(w)
    
    return output


def get_size(start_path='.', unit='bytes'):
    '''
    Calculate the size of a Rdict on Disk.
    Can also be used to calculate the soze of a folder.

    :param start_path: (optional) The path of the folder (default is .)
    :param unit: (optional) unit to display. One of {bytes, kb, mb, gb} (default is bytes)

    :return: Space that the specified folder takes on disk
    '''
    exponents_map = {'bytes': 0, 'kb': 1, 'mb': 2, 'gb': 3}
    if unit not in exponents_map:
        raise ValueError("Must select from ['bytes', 'kb', 'mb', 'gb']")
    total_size_bytes = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            try:
                fp = os.path.join(dirpath, f)
                # Skip if it is symbolic link
                if not os.path.islink(fp):
                    total_size_bytes += os.path.getsize(fp)
            except FileNotFoundError:
                print(f'File was deleted: {f}')

    return f'{round(total_size_bytes / 1024 ** exponents_map[unit], 3)}{unit}'


if __name__ == "__main__":
    # --------------------------------------------------------------------------------------------------------------------------------
    
    # Edit here
    
    
    # database with all crawled websites
    db = Rdict('../data/crawl_data')
    
    with open('../data/crawl_state.pkl', 'rb') as f:
        crawl_state = pickle.load(f)
    
    # get all relevant documents
    #  
    # urls need to be all of them
    #
    # 
    urls = list(crawl_state['visited']) # also include english rejected
    print(len(urls))
    
    print(f"Preprocessing {len(urls)} urls")
    # This is the output Rdict
    db_preprocessed = Rdict("../data/prep_data")
    
    i = 1
    start_ts = time.time()
    for url in urls:
        text = db[url]
        if text:
            db_preprocessed[url] = preprocess(text)
        
        if i  % 50 == 0:
            duration_s = time.time() - start_ts
            total_duration_s = (duration_s / i) * len(urls)
            print(f"Preprocessed {i} of {len(urls)} Websites.")
            print(f"Estimated time: {duration_s / 60:.1f}m of {total_duration_s / 60:.1f}m")
            db_preprocessed.flush()
        i += 1
    
    
    # --------------- Don't forget to close Rdict --------------------------
    db_preprocessed.close()
    db.close()