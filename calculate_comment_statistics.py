# definition of functions to calculate the control variable statistics for each project
# covers the variables that are described in Wang: "Understanding importance of interaction"

import pandas as pd
import os
import datetime as dt
import pytz  # for timezone handling
import nltk  # for natural language processing
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords


# import spacy  # for lemmatization

pd.options.mode.chained_assignment = None
target_tz = pytz.timezone("UTC")

creator_names = ["Projektgründer", "ProjektgründerSuperbacker"]


def calculate_reply_ratio(loc_comments_df):
    # calculate total number of comments that have been made by backers
    # (NOT answers, but original comments)

    comments_of_backers = loc_comments_df[
        (loc_comments_df["title"].isin(creator_names) == False) &
        (loc_comments_df["answerID"].isna())
    ]
    n_comments_of_backers = len(comments_of_backers.index)

    # calculate number of comments where the projedt initiator has replied at least once
    answers_of_creator = loc_comments_df[
        (loc_comments_df["title"].isin(creator_names) == False) &
        (loc_comments_df["answerID"].notnull())
    ]
    n_comments_answered_by_creator = answers_of_creator["commentID"].nunique()

    # calculate reply ratio
    try:
        reply_ratio = n_comments_answered_by_creator / n_comments_of_backers
    except ZeroDivisionError:
        reply_ratio = float("NaN")

    return reply_ratio


def calculate_reply_speed(loc_comments_df, projectID):
    # calculates the average reply speed in hours
    # only takes into account comments of backers that have been answered by the project creator
    # if project creator has answered multiple times to that comment, only the first answer is taken into account
    # if project creator has not answered, it is also not taken into account

    comments_of_backers = loc_comments_df[
        (loc_comments_df["title"] != "Projektgründer") &
        (loc_comments_df["answerID"].isna()) &
        (loc_comments_df["projectID"] == projectID)
        ].reset_index()

    comments_of_backers = comments_of_backers[["commentID", "answerID", "projectID", "title", "utcTime"]]

    answers_of_creator = loc_comments_df[
        (loc_comments_df["title"] == "Projektgründer") &
        (loc_comments_df["projectID"] == projectID) &
        (loc_comments_df["answerID"].notnull())
        ].groupby("commentID").first().reset_index()

    answers_of_creator = answers_of_creator[["commentID", "answerID", "projectID", "title", "utcTime"]]

    comments_of_backers_with_answers_of_creators = answers_of_creator.merge(comments_of_backers,
                                                                            on=["projectID", "commentID"], how="left")

    comments_of_backers_with_answers_of_creators["replySpeed"] = \
        comments_of_backers_with_answers_of_creators["utcTime_x"] - comments_of_backers_with_answers_of_creators[
            "utcTime_y"]

    average_reply_speed_hours = comments_of_backers_with_answers_of_creators[
                                    "replySpeed"].dt.total_seconds().mean() / 3600

    return average_reply_speed_hours


def calculate_reply_length(loc_comments_df, projectID):
    # calculates the average number of words in a comment or reply of a CREATOR in the project
    # AS OF NOW: also counts e.g. ":)" or "xx" as words which in fact are not words
    # alternative: paper used number of bytes in a comment (had english and chinese comments)

    loc_comments_df = loc_comments_df[
        (loc_comments_df["title"] == "Projektgründer") &
        (loc_comments_df["projectID"] == projectID)]

    loc_comments_df["replyLength"] = loc_comments_df.apply(lambda x: len(x["content"].split()), axis=1)
    average_reply_length = loc_comments_df["replyLength"].mean()

    return average_reply_length


def calculate_comment_length(loc_comments_df, projectID):
    # calculate the average number of words in a comment or reply of a BACKER in this project

    loc_comments_df = loc_comments_df[
        (loc_comments_df["title"] != "Projektgründer") &
        (loc_comments_df["projectID"] == projectID)
    ]

    loc_comments_df["commentLength"] = loc_comments_df.apply(lambda x: len(x["content"].split()), axis = 1)
    average_comment_length = loc_comments_df["commentLength"].mean()

    return average_comment_length


def calculate_comment_sentiment(loc_comments_df, projectID):
    # calculate the average sentiment of comments, at the moment differ between backer and project creator

    loc_comments_df["sentiment"] = loc_comments_df.apply(lambda x: sid.polarity_scores(x["content"])["compound"], axis=1)

    backer_comments = loc_comments_df[loc_comments_df["title"] != "Projektgründer"]
    creator_comments = loc_comments_df[loc_comments_df["title"] == "Projektgründer"]

    average_backer_sentiment = backer_comments["sentiment"].mean()
    average_creator_sentiment = creator_comments["sentiment"].mean()

    return average_backer_sentiment, average_creator_sentiment


if __name__ == '__main__':

    from process_xml_files import transform_xml_to_dataframe

    nltk.download("punkt")  # punkt resource is a pre-trained model that helps to tokenize words and sentences
    nltk.download("stopwords")

    # define sentiment intensity analyzer model
    sid = SentimentIntensityAnalyzer()

    print("hello 1")

    print("hello 2")
    stop_words = stopwords.words('english')
    print("hello 3")

    # for testing purposes, create some sample data that can be worked on in here - later delete

    directory = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/test/art/comments"
    comments_df = pd.DataFrame([])

    for entry in os.scandir(directory):
        # files with 37 byte size do not contain comments and are directly skipped
        if entry.path.endswith(".xml") and entry.is_file() and entry.stat().st_size > 37:
            print(entry.stat().st_size)
            comments_df = comments_df.append(transform_xml_to_dataframe(entry))

    print(comments_df)

    # test function
    print(calculate_reply_ratio(
            loc_comments_df=comments_df,
            projectID="pixeloccult"
    ))


    # test function
    print(calculate_reply_speed(
        loc_comments_df=comments_df,
        projectID="pixeloccult"
    ))

    # test function
    print(calculate_reply_length(
         loc_comments_df=comments_df,
         projectID="pixeloccult"
     ))

    # test function
    print(calculate_comment_length(
        loc_comments_df=comments_df,
        projectID="pixeloccult"
    ))

    # test function
    print(calculate_comment_sentiment(
        loc_comments_df=comments_df,
        projectID="pixeloccult"
    ))

    comments_df = pd.read_csv(
        "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/test/full_comments_df_art_test.csv")
    comments_df["utcTime"] = pd.to_datetime(comments_df["utcTime"])

    test = comments_df.groupby("projectID").apply(calculate_reply_ratio)
