from rocksdict import Rdict
import time
# from ftlangdetect import detect
from interface import DocInfo

# punctuation that should be removed
punc = '''|!()-[]{};:'",<>./?@#$%^&*_~\\“”’‘'''
# load basic stopword-file
with open('../data/runtime_data/stopwords.txt', 'r') as f:
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


def init_forward_database(doc_db_path: str, forward_db_path: str, batch_size=5000):
    with Rdict(doc_db_path) as doc_db, Rdict(forward_db_path) as forward_db:
        # forward_db with url -> DocInfo[doc_index, doc]
        print("all databases loaded")

        expected_length = 2800000

        iterations = 1
        doc_index = 0
        start_ts = time.time()
        for url, doc in doc_db.items():
            if url in forward_db:
                # already added, this program was already started before
                continue

            if doc: # and doc != "" and (detect(doc.replace('\n', ' '), low_memory=True)['lang'] == 'en'):
                forward_db[url] = DocInfo(doc_index, preprocess(doc))
                # backward_db[doc_index] = url
                doc_index += 1

            if iterations % batch_size == 0:
                duration_s = time.time() - start_ts
                current_speed = (iterations / duration_s) * 60
                print("-----------")
                print(f"Preprocessed {iterations} Websites, {doc_index} of them were non empty.")
                print(f"average documents per minute: {current_speed:.2f}")
                print(f"with the expected length of {expected_length} this will take {(expected_length - iterations) / current_speed:.2f} minutes")
                forward_db.flush()
                # backward_db.flush()
            iterations += 1


def init_backward_database(forward_db_path: str, backward_db_path: str, batch_size=5000):
    with Rdict(forward_db_path) as forward_db, Rdict(backward_db_path) as backward_db:
        # backward_db with doc_index -> url
        iterations = 1
        start_ts = time.time()
        for url, doc_info in forward_db.items():
            if url in backward_db:
                continue

            backward_db[doc_info.doc_index] = url

            if iterations % batch_size == 0:
                duration_s = time.time() - start_ts
                current_speed = (iterations / duration_s) * 60
                print("-----------")
                print(f"Created back link for {iterations} Websites")
                print(f"average documents per minute: {current_speed:.2f}")
                backward_db.flush()
            iterations += 1


# --------------------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    # init_forward_database('../data/crawl_data', '../data/forward_db')
    init_backward_database('../data/runtime_data/forward_db', '../data/runtime_data/backward_db')


