'''
Builds the inverted index

To make it faster and be able to handle crashes, all websites are split into blocks and the inverted index is always built for one block
All blocks have to be merged later (takes about an hour)

'''

import pickle
from rocksdict import Rdict
import time
import numpy as np
import os
import sys
# Add the parent directory to the system path
parent_dir = os.path.abspath(os.path.join(os.getcwd(), '..'))
sys.path.insert(0, parent_dir)
import interface

'''
Inverted index will be a dict of form:

{
 word: {(document, tf), (document, tf), ...})
}

The idf can be caclulated with: len(inv_idx[word])
'''

forward_path = "../data/bm25/words50k"  # use preprocessed database
backward_path = "../data/backward_db"
dest_path = "../data/bm25/inverted100k"
doc_len_path = "../data/bm25/doc_len100k"

# load and create dbs
forward_db = Rdict(forward_path)
# backward_db = Rdict(backward_path)
# inverted = Rdict(dest_path)
# doc_len = Rdict(doc_len_path) # save word_counts of every document
inverted = {}
doc_len = {}

# approach with blocks
block_path = "../data/bm25/inv50_blocks"
base_name = "block-"
base_ending = ".pkl"
block_nr = 1
block_size = 50000

# No preprocessing needed (words100k contains only words that are mentioned by less than 100k documents)

# Begin to build inverted index
count = 1                          # counter for status printing
amount = 2104725                   # amount of keys in forward db (takes 3 min to calculate)
start_ts = time.time()             # timestamp for status printing
for url, info in forward_db.items():
    # Define index, url is too big to save (unnecessray storage)
    idx = info.doc_index
    # Add document length
    doc_len[idx] = len(info.word_list)
    # remove duplicate words for loop
    set_words = set(info.word_list)
    for word in set_words:
        # count tf and put both into inverted index
        # tf can be divided by max_word_count
        tf = len([w for w in info.word_list if w == word])
        # Append every word to inverted index
        # Try to get already existing documents for evey word
        # If KeyError, there are none
        try:
            temp = inverted[word]
        except KeyError:
            temp = set()
        # add current document to temp-set
        # But add index instead of url
        temp.add((idx, tf))
        # update inverted index
        inverted[word] = temp
        

    # document finished, print updates
    if count % 4000 == 0:
        duration_s = time.time() - start_ts
        estimated_duration_s = (duration_s / count) * amount
        print(f"Build inverted index {count} of {amount} Websites.")
        print(f"Estimated time: {duration_s / 60:.1f}m of {estimated_duration_s / 60:.1f}m")
        size_inv = round(sys.getsizeof(inverted) / 1024 ** 3, 3)
        size_len = round(sys.getsizeof(doc_len) / 1024 ** 3, 3)
        print(f"Size of inv. index: {len(inverted.keys())} words.")
        # print(f"Estimated size of inv. index: {size_inv} of {(size_inv / count) * amount:.3f}")
        # print(f"Estimated size of doc_len: {size_len} of {(size_len / count) * amount:.3f}")
        print(f"Block index: {block_nr}")
        print()


    # save block and print info
    if count % block_size == 0:
        with open(block_path + base_name + str(block_nr) + base_ending, 'wb') as f:
            pickle.dump(inverted, f)
        
        block_nr += 1
        inverted = {}
        print(f"Saved {block_nr-1}. Block")

    count += 1

# Save last_block

with open(block_path + base_name + str(block_nr) + base_ending, 'wb') as f:
    pickle.dump(inverted, f)

print("Finished building inverted index")   


with open("../data/bm25/doc_len.pkl", "wb") as f:
    pickle.dump(doc_len, f)


# close databases
forward_db.close()
#backward_db.close()
#inverted.close()
#doc_len.close()