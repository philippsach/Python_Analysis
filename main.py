'''
main.py:
This file is used for running the analysis of our code: coming from XML files on our disk
until we have a file with all the needed features that we want to extract from the comments.
'''

__author__ = "Philipp Sach"
__version__ = "0.0.1"
__status__ = "Prototype"

# Futures

# Built-in/Generic imports
import os

# Libs
import pandas as pd
import numpy as np

# Own modules
import process_xml_files
import calculate_comment_statistics

# User settings
# insert user settings here :) (like path definitions etc.)

category_data = {
    "category": ["art", "comic", "craft", "dance", "design",
                 "fashion", "food", "games", "journalism", "music",
                 "photo", "publishing", "technology", "theater", "video"]
}
category_df = pd.DataFrame(category_data)

category_df["xml_path"] = category_df.apply(
    lambda x: "/Users/philippsach/HiDrive/public/Kickstarter_Data/" +
    x["category"] +
    "/Comments",
    axis=1)

category_df["deadline_query"] = category_df.apply(
    lambda x: "SELECT Project_Nr, Deadline, end_year, end_month, end_day, end_time, comments, withdrawn_comments\
    FROM combined_metadata WHERE (state = 'Successful' OR state = 'FAILED') AND category='" +
    x["category"] + "'",
    axis=1)

replace_dict= {"Superbacker":"Superbacker",
                   "Creator":"Projektgründer",
                   "CreatorSuperbacker":"ProjektgründerSuperbacker",
                   "Collaborator":"Mitarbeiter",
                   "CollaboratorSuperbacker":"MitarbeiterSuperbacker"}


if __name__ == '__main__':
    print("Main script is running.")

    # CHOOSE WHICH CATEGORY TO WORK ON -----
    category = "art"
    xml_path = category_df[category_df["category"] == category].iloc[0, 1]
    deadline_query = category_df[category_df["category"] == category].iloc[0, 2]

    # PROCESS XML FILES --------------------
    # section 1) transform xml to dataframe
    comments_df = process_xml_files.wrapper_process_xml(local_directory=xml_path)

    # section 2) clean german and english names in title
    print("unique titles before replacement: ", comments_df["title"].unique())
    comments_df["title"] = comments_df["title"].map(replace_dict).fillna(comments_df["title"])

    # FILTER TO OBTAIN RELEVANT DATAFRAMES -
    # before end date /

    # CALCULATE COMMENT STATISTICS ---------
    print(comments_df.head())
    print("unique titles after replacement: ", comments_df["title"].unique())

    reply_ratio = comments_df.groupby("projectID").apply(calculate_comment_statistics.calculate_reply_ratio)

    print(reply_ratio.head(100))

