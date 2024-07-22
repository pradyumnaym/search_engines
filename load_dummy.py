import pickle
from . import interface

def get_file():
    with open("../data/falafel_result.pkl", "rb") as f:
        result = pickle.load(f)
        return result