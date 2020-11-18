# definition of functions to calculate the control variable statistics for each project
# covers the variables that are described in Wang: "Understanding importance of interaction"

import pandas as pd
import os
import datetime as dt
import pytz  # for timezone handling
import locale

target_tz = pytz.timezone("UTC")

# for testing purposes, create some sample data that can be worked on in here - later delete
from process_xml_files import transform_xml_to_dataframe
directory = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/test/art/comments"
comments_df = pd.DataFrame([])

for entry in os.scandir(directory):
    # files with 37 byte size do not contain comments and are directly skipped
    if entry.path.endswith(".xml") and entry.is_file() and entry.stat().st_size>37:
        print(entry.stat().st_size)
        comments_df = comments_df.append(transform_xml_to_dataframe(entry))

print(comments_df)

def get_comments_df():
    # this function should obtain the data from the MySQL database
    # and change it into a pandas dataframe format
    # still to be decided: if this function only downloads for 1 project or the whole thing

    return 1

def calculate_reply_ratio(loc_comments_df, projectID):
    # calculate total number of comments that have been made by backers
    # (NOT answers, but original comments)
    # STILL TBD - what happens if there are 0 comments? what is the reply ratio then?

    comments_of_backers = loc_comments_df[
        (loc_comments_df["title"] != "Projektgründer") &
        (loc_comments_df["answerID"].isna()) &
        (loc_comments_df["projectID"] == projectID)
    ]
    n_comments_of_backers = len(comments_of_backers.index)

    # calculate number of comments where the projedt initiator has replied at least once
    answers_of_creator = loc_comments_df[
        (loc_comments_df["title"] == "Projektgründer") &
        (loc_comments_df["projectID"] == projectID) &
        (loc_comments_df["answerID"].notnull())
    ]
    n_comments_answered_by_creator = answers_of_creator["commentID"].nunique()

    # calculate reply ratio
    reply_ratio = n_comments_answered_by_creator / n_comments_of_backers

    return reply_ratio


def calculate_reply_speed(loc_comments_df, projectID):
    # calculates the average reply speed in hours
    # only takes into account comments of backers that have been answered by the project creator
    # if project creator has answered multiple times to that comment, only the first answer is taken into account
    # if project creator has not answered, it is also not taken into account

    comments_of_backers = loc_comments_df[
        (loc_comments_df["title"] != "Projektgründer") &
        (loc_comments_df["answerID"].isna()) &
        (loc_comments_df["projectID"] == "pixeloccult")
        ].reset_index()

    comments_of_backers = comments_of_backers[["commentID", "answerID", "projectID", "title", "utcTime"]]

    answers_of_creator = loc_comments_df[
        (loc_comments_df["title"] == "Projektgründer") &
        (loc_comments_df["projectID"] == "pixeloccult") &
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


def calculate_reply_length():
    # they used number of bytes in a comment, can we also just do it like this?
    return 1


# test function
print(calculate_reply_ratio(
        loc_comments_df=comments_df,
        projectID="pixeloccult"
))

# tests for reply speed
print(comments_df["strTime"].values[0])
print(comments_df["strTime"].values[1])
print(comments_df["utcTime"].values[1] - comments_df["utcTime"].values[0])

# test function
print(calculate_reply_speed(
    loc_comments_df=comments_df,
    projectID="pixeloccult"
))
