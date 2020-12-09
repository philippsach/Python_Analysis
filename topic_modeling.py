import re

import matplotlib
from ast import literal_eval
import mysql.connector
import numpy as np
import pandas as pd
from pprint import pprint
import nltk
import pymysql

# gensim
import gensim
import gensim.corpora as corpora
from gensim.utils import simple_preprocess
from gensim.models import CoherenceModel

# spacy for lemmatization
import spacy
import os

# plotting tools
import pyLDAvis
import pyLDAvis.gensim
import matplotlib.pyplot as plt


import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)

import warnings
warnings.filterwarnings("ignore",category=DeprecationWarning)
from nltk.corpus import stopwords
stop_words = stopwords.words('english')


def remove_stopwords(texts):
    return [[word for word in simple_preprocess(str(doc)) if word not in stop_words] for doc in texts]


def make_bigrams(texts, bigram_mod):
    return [bigram_mod[doc] for doc in texts]


def make_trigrams(texts, bigram_mod, trigram_mod):
    return [trigram_mod[bigram_mod[doc]] for doc in texts]


def lemmatization(texts, nlp, allowed_postags=["NOUN", "ADJ", "VERB", "ADV"]):
    """https://spacy.io/api/annotation"""
    texts_out = []
    for sent in texts:
        doc = nlp(" ".join(sent))
        texts_out.append([token.lemma_ for token in doc if token.pos_ in allowed_postags])
    return texts_out


def get_all_info_from_database():
    cnx = pymysql.connect(user="phil_sach",
                          password="entthesis2020",
                          host="85.214.204.221",
                          database="thesis")

    cursor = cnx.cursor()
    query = "Select Project_Nr, Long_Description from thesis.design Where state = \"erfolgreich\" or state = " \
            "\"Failed\" Limit 10;"
    cursor.execute(query)
    data_text = {}
    for (nr, description) in cursor:
        print("nr: ", nr)  # , "; description: ", description)
        des = description.replace('\\n', " ")
        #des = literal_eval(des)
        data_text.update({nr: des})
    cursor.close()
    cnx.close()
    processed_docs = []
    print(data_text)
    for key in data_text:
        processed_docs.append(gensim.utils.simple_preprocess(data_text[key], deacc=True))

    return processed_docs


def compute_coherence_values(dictionary, corpus, texts, limit, start=2, step=3):
    """
    Compute c_v coherence for various number of topics
    Parameters:
    ----------
    dictionary : Gensim dictionary
    corpus : Gensim corpus
    texts : List of input texts
    limit : Max num of topics
    Returns:
    -------
    model_list : List of LDA topic models
    coherence_values : Coherence values corresponding to the LDA model with respective number of topics
    """
    mallet_path = 'C:\\new_mallet\\mallet-2.0.8\\bin\\mallet.bat'
    coherence_values = []
    model_list = []
    for num_topics in range(start, limit, step):
        model = gensim.models.wrappers.LdaMallet(mallet_path, corpus=corpus, num_topics=num_topics, id2word=dictionary)
        model_list.append(model)
        coherencemodel = CoherenceModel(model=model, texts=texts, dictionary=dictionary, coherence='c_v')
        coherence_values.append(coherencemodel.get_coherence())

    return model_list, coherence_values


def format_topics_sentences(ldamodel, corpus, texts):
    # Init output
    sent_topics_df = pd.DataFrame()

    # Get main topic in each document
    for i, row in enumerate(ldamodel[corpus]):
        row = sorted(row, key=lambda x: (x[1]), reverse=True)
        # Get the Dominant topic, Perc Contribution and Keywords for each document
        for j, (topic_num, prop_topic) in enumerate(row):
            if j == 0:  # => dominant topic
                wp = ldamodel.show_topic(topic_num)
                topic_keywords = ", ".join([word for word, prop in wp])
                sent_topics_df = sent_topics_df.append(pd.Series([int(topic_num), round(prop_topic,4), topic_keywords]), ignore_index=True)
            else:
                break
    sent_topics_df.columns = ['Dominant_Topic', 'Perc_Contribution', 'Topic_Keywords']

    # Add original text to the end of the output
    contents = pd.Series(texts)
    sent_topics_df = pd.concat([sent_topics_df, contents], axis=1)
    return(sent_topics_df)


def main():
    processed_docs = get_all_info_from_database()
    print(processed_docs)
    pprint("Got Docs")
    bigram = gensim.models.Phrases(processed_docs, min_count=5, threshold=100)
    trigram = gensim.models.Phrases(bigram[processed_docs], threshold=100)

    bigram_mod = gensim.models.phrases.Phraser(bigram)
    trigram_mod = gensim.models.phrases.Phraser(trigram)
    pprint("Got Mods")

    # Remove Stop Words
    data_words_nostops = remove_stopwords(processed_docs)

    # Form Bigrams
    data_words_bigrams = make_bigrams(data_words_nostops, bigram_mod)

    # Initialize spacy 'en' model, keeping only tagger component (for efficiency)
    nlp = spacy.load('en', disable=['parser', 'ner'])

    # Do lemmatization keeping only noun, adj, vb, adv
    data_lemmatized = lemmatization(data_words_bigrams, nlp, allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV'])
    pprint("Lemmatized")

    # Create Dictionary
    id2word = corpora.Dictionary(data_lemmatized)

    # Create Corpus
    texts = data_lemmatized
    pprint("Corpus Created")

    # Term Document Frequency
    corpus = [id2word.doc2bow(text) for text in texts]
    '''
    # Build LDA model
    lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus,
                                               id2word=id2word,
                                               num_topics=20,
                                               random_state=100,
                                               update_every=1,
                                               chunksize=100,
                                               passes=10,
                                               alpha='auto',
                                               per_word_topics=True)
    
     # Print the Keyword in the 10 topics
    pprint(lda_model.print_topics())
    doc_lda = lda_model[corpus]
    # Compute Perplexity
    print('\nPerplexity: ', lda_model.log_perplexity(corpus))  # a measure of how good the model is. lower the better.
    # Compute Coherence Score
    print("Doing This")
    coherence_model_lda = CoherenceModel(model=lda_model, texts=data_lemmatized, dictionary=id2word, coherence='c_v')
    print("Doing That")
    coherence_lda = coherence_model_lda.get_coherence()
    print('\nCoherence Score: ', coherence_lda)
    # Visualize the topics
    vis = pyLDAvis.gensim.prepare(lda_model, corpus, id2word)
    pyLDAvis.save_html(vis, 'LDA_Visualization.html')
    '''
    #os.environ["MALLET_HOME"] = "/Users/philippsach/Documents/Uni/Masterarbeit/Python_Analysis/aux_files/mallet-2.0.8"
    os.environ.update({"MALLET_HOME": "/Users/philippsach/Documents/Uni/Masterarbeit/Python_Analysis/aux_files/mallet-2.0.8"})
    # path = os.getcwd()
    # mallet_relative_path = "aux_files/mallet-2.0.8/bin/mallet.bat"
    # mallet_path = os.path.join(path, mallet_relative_path) # update this path
    mallet_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Python_Analysis/aux_files/mallet-2.0.8/bin/mallet"
    ldamallet = gensim.models.wrappers.LdaMallet(mallet_path, corpus=corpus, num_topics=15, id2word=id2word)

    # Show Topics
    #pprint(ldamallet.show_topics(formatted=False))

    # Compute Coherence Score
    coherence_model_ldamallet = CoherenceModel(model=ldamallet, texts=data_lemmatized, dictionary=id2word,
                                               coherence='c_v')
    coherence_ldamallet = coherence_model_ldamallet.get_coherence()
    print('\nCoherence Score: ', coherence_ldamallet)

    '''
    model_list, coherence_values = compute_coherence_values(dictionary=id2word, corpus=corpus, texts=data_lemmatized,
                                                            start=2, limit=40, step=6)
    
    # Show graph
    limit = 40;
    start = 2;
    step = 6;
    x = range(start, limit, step)
    plt.plot(x, coherence_values)
    plt.xlabel("Num Topics")
    plt.ylabel("Coherence score")
    plt.legend(("coherence_values"), loc='best')
    plt.savefig('coherence.png')
    # Print the coherence scores
    for m, cv in zip(x, coherence_values):
        print("Num Topics =", m, " has Coherence Value of", round(cv, 4))
    '''
    df_topic_sents_keywords = format_topics_sentences(ldamodel=ldamallet, corpus=corpus, texts=processed_docs)

    # Format
    df_dominant_topic = df_topic_sents_keywords.reset_index()
    df_dominant_topic.columns = ['Document_No', 'Dominant_Topic', 'Topic_Perc_Contrib', 'Keywords', 'Text']

    # Show
    df_dominant_topic.head(10)
    print(df_dominant_topic)


if __name__ == "__main__":
    main()
