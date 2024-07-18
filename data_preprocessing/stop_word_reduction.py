from rocksdict import Rdict
import time
from ftlangdetect import detect
from interface import DocInfo

# punctuation that should be removed
punc = '''|!()-[]{};:'",<>./?@#$%^&*_~\\“”’‘'''


def remove_punctuation(word):
    return ''.join([char for char in word if char not in punc])


# remove stopwords (basic stopword-file)
with open('./stopwords.txt', 'r') as f:
    stopwords = f.read().splitlines()

# remove numbers (more than 3-digit)
numbers = "0123456789"


def is_number(word):
    num_count = 0
    idx = 0
    while num_count <= 3 and idx < len(word):
        if word[idx] in numbers:
            num_count += 1
        idx += 1
    return num_count > 3


# Detect if character is not ascii
ascii_chars = '''!"#$%&()*+'-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~€‚ƒ„…†‡ˆ‰Š‹ŒŽ“”•–—˜™š›œžŸ¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ'''


def is_sus(word):
    return any([c not in ascii_chars for c in word])


# preprocess that text
def preprocess(text):
    # should work on lists and strings
    if isinstance(text, str):
        words = text.split(" ")
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


def init_forward_database(doc_db_path: str, forward_db_path: str, batch_size=5000):
    #  with Rdict(doc_db_path) as doc_db, Rdict(forward_db_path) as forward_db, Rdict(backward_db_path) as backward_db:
    with Rdict(doc_db_path) as doc_db, Rdict(forward_db_path) as forward_db:
        # db with url -> DocInfo[doc_index, doc]
        # db with doc_index -> url
        print("all databases loaded")

        expected_length = 2800000

        iterations = 1
        doc_index = 0
        start_ts = time.time()
        for url, doc in doc_db.items():
            if url in forward_db:
                # already added, this program was already started before
                continue

            if doc and (detect(doc.replace('\n', ' '), low_memory=True)['lang'] == 'en'):
                forward_db[url] = DocInfo(doc_index, preprocess(doc))
                # backward_db[doc_index] = url
                doc_index += 1

            if iterations % batch_size == 0:
                duration_s = time.time() - start_ts
                current_speed = (iterations / duration_s) * 60
                print("-----------")
                print(f"Preprocessed {iterations} Websites, {doc_index} of them were non empty.")
                print(f"average documents per minute: {current_speed:.2f}")
                # print(f"with the expected length of {expected_length} this will take {(expected_length - iterations) / current_speed:.2f} minutes")
                forward_db.flush()
                # backward_db.flush()
            iterations += 1

def init_backward_database(forward_db_path: str, backward_db_path: str, batch_size=5000):
    with Rdict(forward_db_path) as forward_db, Rdict(backward_db_path) as backward_db:
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
                # print(f"with the expected length of {expected_length} this will take {(expected_length - iterations) / current_speed:.2f} minutes")
                # forward_db.flush()
                backward_db.flush()
            iterations += 1


# --------------------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    # init_forward_database('../data/crawl_data', '../data/forward_db')
    init_backward_database('../data/forward_db', '../data/backward_db')


