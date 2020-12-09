from pprint import pprint
import pymysql
import pandas as pd

# gensim
import gensim
import gensim.corpora as corpora
from gensim.utils import simple_preprocess
from gensim.models import CoherenceModel

# spacy for lemmatization
import spacy




test_dict = {
    "10462846728": "This is a really nice description of a project",
    "123456": "Why do we need even more text data?",
    "987654": "I am getting tired of this testing..."
}

for key in test_dict:
    words = []
    print(key, "-->", test_dict[key])
    for word in test_dict[key].split(' '):
        words.append(word)
    print(words)

processed_docs = []
for key in test_dict:
    processed_docs.append(gensim.utils.simple_preprocess(test_dict[key], deacc=True))

print("processed docs: ", processed_docs)

comments_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/test/full_comments_df_art_test.csv"

comments_df = pd.read_csv(comments_path)
comments_df["title"] = comments_df["title"].astype(str)
comments_df["projectID"] = comments_df["projectID"].astype(str)
comments_df["content"] = comments_df["content"].astype(str)
comments_df_backers = comments_df[comments_df["title"].str.contains("Creator")==False]

comments_df_backers = comments_df_backers[["projectID", "content"]]

print(comments_df["title"].unique())

joined_comments_df_backers = comments_df_backers.groupby("projectID")["content"].apply(" ".join).reset_index()

comments_dict = dict(zip(joined_comments_df_backers.projectID, joined_comments_df_backers.content))

for (nr, description) in comments_dict:
    print(nr)