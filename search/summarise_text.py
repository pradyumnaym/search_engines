import spacy
from multiprocessing import Pool

try:
    nlp = spacy.load('en_core_web_lg')
except OSError:
    print('Downloading language model for the spaCy POS tagger\n'
        "(don't worry, this will only happen once)")
    from spacy.cli import download
    download('en_core_web_lg')
    nlp = spacy.load('en_core_web_lg')

def get_relevant_sentences(text, keywords):
    """
    Returns top 5 sentences from the text that are most relevant to the keywords.

    Arguments
    ---------
    text: str, the text to summarize
    keywords: str, the keywords to extract from the text

    Returns
    -------
    list of str, the top 5 most relevant sentences from the text

    """

    # Keywords for extraction
    keywords = keywords.split()

    # Process text with spaCy
    doc = nlp(text)

    # Convert keywords to spaCy tokens
    keyword_tokens = [nlp(keyword) for keyword in keywords]

    # Function to calculate similarity score for a sentence
    def sentence_similarity(sentence, keywords):
        return max(sentence.similarity(keyword) for keyword in keywords)

    sentences = list(doc.sents)
    sentences = [ (sent, sentence_similarity(sent, keyword_tokens)) for sent in sentences ]
    sentences.sort(key=lambda x: x[1], reverse=True)

    # filter out low similarity sentences
    sentences = [sent[0] for sent in sentences if sent[1] > 0.6]
    sentences = [str(sent) for sent in sentences]
    
    return sentences[:5]

def keywords_wrapper(keywords):
    return lambda text: get_relevant_sentences(text, keywords)

def get_relevant_sentences_parallel(texts, keywords):
    with Pool() as pool:
        return pool.map(keywords_wrapper(keywords), texts)
    
if __name__ == "__main__":
    import random, pickle

    with open("../data/values_sample.pkl", "rb") as f:
        values = pickle.load(f)

    text = random.choice(values)
    keywords = "research development"

    print(get_relevant_sentences(text, keywords))