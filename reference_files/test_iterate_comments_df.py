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
        for question in data_text[key]:

            print("Original document: ")
            words = []
            for word in data_text[key][question].split(' '):
                words.append(word)
            print(words)
            print('\n\n tokenized and lemmatized document: ')
            print(preprocess(data_text[key][question]))
            break

            processed_docs.append(gensim.utils.simple_preprocess(data_text[key][question], deacc=True))
    return processed_docs


def main():
    processed_docs = get_all_info_from_database()
    print(processed_docs)
    pprint("Got Docs")


#if __name__ == "__main__":
 #   main()


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