import pickle


with open('../data/crawling_data/crawl_state.pkl', 'rb') as f:
    d = pickle.load(f)

new_frontier = []
seen_urls = set()
print("original frontier length: ", len(d['frontier']))
print("Discovered urls: ", len(d['all_discovered_urls']))

print("Unique frontier URLs", len(set([x[0] for x in d['frontier']])))

for item in d['frontier']:
    if item[0] not in d['all_discovered_urls'] and item[0] not in seen_urls:
        new_frontier.append(item)
        seen_urls.add(item[0])

d['frontier'] = new_frontier
print(d['frontier'][0])
print("New frontier length: ", len(d['frontier']))

with open('../data/crawling_data/crawl_state.pkl', 'wb') as f:
    pickle.dump(d, f)
