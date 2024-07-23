'''
File to cut off words that are too frequent
With this, the size of index and inverted index can be reduced
'''

import pickle
from rocksdict import Rdict
import time
import os
import sys
# Add the parent directory to the system path
parent_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))
sys.path.insert(0, parent_dir)
import interface
from data_preprocessing.general_preprocessing import get_size


#forward_path = "../data/forward_db"
forward_path = "../data/bm25/words50k"
count_path = "../data/word_counts.pkl"

#output_path = "../data/bm25/words100k"
output_path = "../data/bm25/words10k"

#cut_off_point = 100000
cut_off_point = 10000  

# Forward:
# url -> DocInfo(doc_index, doc_text)
#
# Backward:
# doc_index -> url

forward_db = Rdict(forward_path)
clean_db = Rdict(output_path)

# load word counts
with open(count_path, "rb") as f:
    word_counts = pickle.load(f)

# Define which words should be included
#
# Only include words that are mentioned by less than 100k documents      
words_full = word_counts.keys()
words_small = [w for w,c in word_counts.items() if c < cut_off_point]
words_out = [w for w,c in word_counts.items() if c >= cut_off_point]

# Compare sizes
print(f"Full words count: {len(words_full)}")
print(f"Words count with less than {cut_off_point} mentions: {len(words_small)}")
print(f"Words count removed: {len(words_out)}")

# remove words that are already removed
words_out50 = [w for w,c in word_counts.items() if c >= 50000]
words_out = [w for w,c in word_counts.items() if (c >= cut_off_point and w not in words_out50)]
print(f"Words that need to be removed: {len(words_out)}")


# Begin to build inverted index
count = 1                          # counter for status printing
amount = 2104725                   # amount of keys in forward db (takes 3 min to calculate)
start_ts = time.time()             # timestamp for status printing
for url, info in forward_db.items():
    w_list = [w for w in info.word_list if w not in words_out]
    doc_info = interface.DocInfo(info.doc_index, w_list)
    clean_db[url] = doc_info

    # document finished, print updates
    if count % 4000 == 0:
        duration_s = time.time() - start_ts
        estimated_duration_s = (duration_s / count) * amount
        print(f"Preprocessed {count} of {amount} Websites.")
        print(f"Estimated time: {duration_s / 60:.1f}m of {estimated_duration_s / 60:.1f}m")


    count += 1


print(f"Finished preprocessing in {(time.time() - start_ts) / 60:.1f}m.")
size_og = get_size(forward_path, "gb")
size_clean = get_size(output_path, "gb")
print(f"Size of original database: {size_og}.")
print(f"Size of cleaned db: {size_clean}.")

forward_db.close()
clean_db.close()