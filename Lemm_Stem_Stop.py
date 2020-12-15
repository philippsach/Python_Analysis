#%% TOPIC MODELING MODEL 2 --- Test with Technology n=1000
print("Step 1-3: Data Import – STARTED")
#   General imports
import re
import numpy as np
import pandas as pd
from pprint import pprint
import time
import json

#   Imports for GENSIM
import gensim
import gensim.corpora as corpora
from gensim.utils import simple_preprocess
from gensim.models import CoherenceModel

#   Imports for Spacy
import spacy

#   Imports for Plotting Tools
#   Plotting tools
import pyLDAvis
import pyLDAvis.gensim  # don't skip this
import matplotlib.pyplot as plt
#%matplotlib inline

#   Enable logging for gensim - optional
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)

import warnings
warnings.filterwarnings("ignore",category=DeprecationWarning)

######===========================================================================================TOPIC MODELING SETTINGS

num_of_topics = 40
ran_state = 100         #FOR:   gensim model
ran_seed = 1            #FOR:   mallet model
num_of_chunk = 100
num_of_passes = 10

######==================================================================================================================

time_started = time.asctime()
print("\n\nStep 1-4: Data Import – FINISHED")
print("\n\nThe LDA-Calculation started at: " + time_started)
print("\n\nContinue with: Step 5")

# NLTK Stop words import process
print("Step 5: Stop Word Import – STARTED")
from nltk.corpus import stopwords
stop_words = stopwords.words('english')
stop_words.extend(['from', 'subject', 're', 'edu', 'use'])

print("\n\nStep 5: Stop Word Import – STARTED")
print("\n\nContinue with: Step 6")
# Column1: Project_Name
# Column2:Kategorie_Nr
# Column3: Long_Description

# Import Dataset
print("\n\nStep 6: Data Import – STARTED")

df = pd.read_json("/Users/daniel_ent/Desktop/Paper2_Calc/Syrine/Prepocessing/All_Data_Video_compl_prepo.json")              #Work Macbook
#df = pd.read_json("/Users/dd_wrk/PycharmProjects/Topics2/files/technology_08_07_2020_compl.json")       #Priv Macbook
#df = pd.read_excel("/Users/daniel_ent/Desktop/Paper2_Calc/Syrine/Prepocessing/All_Data_Video_compl_prepo.xlsx")

df.head()
#To view the database in pycharm, right-click on df (bottom right corner of screen) and "view as dataframe"

print("\n\nStep 6: Data Import – FINISHED")
print("\n\nContinue with: Step 7")
print("\n\nStep 7: List conversion & Removing Process (E-Mail; New Lines; Single Quotes) – STARTED")
# Convert to list
data = df.Video_transc.values.tolist()

# Remove Emails
data = [re.sub('\S*@\S*\s?', '', sent) for sent in data]

# Remove new line characters
data = [re.sub('\s+', ' ', sent) for sent in data]

# Remove distracting single quotes
data = [re.sub("\'", "", sent) for sent in data]

pprint(data[:1])

print("\n\nStep 7: List conversion & Removing Process (E-Mail; New Lines; Single Quotes) – FINISHED")
print("\n\nContinue with: Step 8")

print("Step 8: Tokanization – STARTED")
def sent_to_words(sentences):
    for sentence in sentences:
        yield(gensim.utils.simple_preprocess(str(sentence), deacc=True))  # deacc=True removes punctuations

data_words = list(sent_to_words(data))

print(data_words[:1])

print("\n\nStep 8: Tokanization – FINISHED")
print("\n\nContinue with: Step 9")

# Build the bigram and trigram models
print("\n\nStep 9: Bi- & Trigraming – STARTED")

bigram = gensim.models.Phrases(data_words, min_count=5, threshold=100) # higher threshold fewer phrases.
trigram = gensim.models.Phrases(bigram[data_words], threshold=100)

# Faster way to get a sentence clubbed as a trigram/bigram
bigram_mod = gensim.models.phrases.Phraser(bigram)
trigram_mod = gensim.models.phrases.Phraser(trigram)

# See trigram example
print(trigram_mod[bigram_mod[data_words[0]]])
time_at10 = time.asctime()
print("\n\nStep 9: Bi- & Trigraming – FINISHED")
print("\n\nThe time after Step 9 was finished was: " + time_at10)
print("\n\nContinue with: Step 10")
print("\n\nStep 10: Bigramm and Trigramm implementation after Tokanization – STARTED")

# Define functions for stopwords, bigrams, trigrams and lemmatization
def remove_stopwords(texts):
    return [[word for word in simple_preprocess(str(doc)) if word not in stop_words] for doc in texts]

def make_bigrams(texts):
    return [bigram_mod[doc] for doc in texts]

def make_trigrams(texts):
    return [trigram_mod[bigram_mod[doc]] for doc in texts]

def lemmatization(texts, allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV']):
    """https://spacy.io/api/annotation"""
    texts_out = []
    for sent in texts:
        doc = nlp(" ".join(sent))
        texts_out.append([token.lemma_ for token in doc if token.pos_ in allowed_postags])
    return texts_out

#Now we call the functions in our specified order

# Remove Stop Words
data_words_nostops = remove_stopwords(data_words)

# Form Bigrams
data_words_bigrams = make_bigrams(data_words_nostops)

# Initialize spacy 'en' model, keeping only tagger component (for efficiency)
# python3 -m spacy download en
nlp = spacy.load('en', disable=['parser', 'ner'])

# Do lemmatization keeping only noun, adj, vb, adv
data_lemmatized = lemmatization(data_words_bigrams, allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV'])

print(data_lemmatized[:1])

data_lem = pd.DataFrame({"Long_Desc Prepocessed":data_lemmatized})
#data_lem = pd.DataFrame({"Long_Desc Prepocessed":data_lemmatized})

data_comp = df.join(data_lem)
data_comp["Long_Desc Prepocessed New"] = [",".join(map(str, l)) for l in data_comp["Long_Desc Prepocessed"]]
#data_comp["Long_Desc Prepocessed New"] = [",".join(map(str, l)) for l in data_comp["Long_Desc Prepocessed"]]

# SET FILENAME HERE
data_comp.to_json("games2_3row_prepo.json", orient= "records")

print("Finished process and saved CSV to specified location")
