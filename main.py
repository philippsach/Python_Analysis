""""
main.py:
This file is used for running the analysis of our code: coming from XML files on our disk
until we have a file with all the needed features that we want to extract from the comments.
"""

__author__ = "Philipp Sach"
__version__ = "0.0.1"
__status__ = "Prototype"

# Futures

# Built-in/Generic imports
import os

# Libs
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


# Own modules
import process_xml_files
import match_xml_and_database
import calculate_comment_statistics
import calculate_psycap

# User settings
# TODO: set creator names here instead of within the single files

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

category_df["query"] = category_df.apply(
    lambda x: "SELECT Project_Nr, Deadline, end_year, end_month, end_day, end_time, comments, withdrawn_comments\
    FROM combined_metadata WHERE (state = 'Successful' OR state = 'FAILED') AND category='" +
    x["category"] + "'",
    axis=1)

replace_dict = {"Superbacker": "Superbacker",
                "Creator": "Projektgründer",
                "CreatorSuperbacker": "ProjektgründerSuperbacker",
                "Collaborator": "Mitarbeiter",
                "CollaboratorSuperbacker": "MitarbeiterSuperbacker"}

sqlEngine = create_engine("mysql+pymysql://phil_sach:entthesis2020@85.214.204.221/thesis")


if __name__ == '__main__':
    print("Main script is running.")

    # CHOOSE WHICH CATEGORY TO WORK ON -----
    category = "art"
    xml_path = category_df[category_df["category"] == category].iloc[0, 1]
    query = category_df[category_df["category"] == category].iloc[0, 2]


    # PROCESS XML FILES --------------------
    # section 1) transform xml to dataframe
    comments_df = process_xml_files.wrapper_process_xml(local_directory=xml_path)

    # section 2) cleaning of german and english names in title and data types
    print("unique titles before replacement: ", comments_df["title"].unique())
    comments_df["title"] = comments_df["title"].map(replace_dict).fillna(comments_df["title"])
    print("unique titles after replacement: ", comments_df["title"].unique())
    comments_df["utcTime"] = pd.to_datetime(comments_df["utcTime"])

    # section 3) cleaning of the content part
    comments_df["content"] = comments_df["content"].str.replace("\S*@\S*\s?", "", regex=True)  # email-addresses
    comments_df["content"] = comments_df["content"].str.replace("\s+", "",regex=True)  # new line characters
    comments_df["content"] = comments_df["content"].str.replace("\'", "", regex=True)  # distracting single quotes
    comments_df["content"] = comments_df["content"].str.replace(
        r"(http(s)?:\/\/.)?(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)"
        , "", regex=True)  # urls

    # TODO: think if I have to replace also emojis etc. or not (could also say this could be used by sentiment analyzer)
    # like ":)" is positive while ":(" is negative etc.

    # FILTER TO OBTAIN RELEVANT DATAFRAMES -
    # section 1) differentiate comments before and after deadline
    project_metadata = pd.read_sql(query, con=sqlEngine)
    project_metadata["utc_deadline"] = match_xml_and_database.wrapper_calc_deadline(loc_project_metadata=project_metadata)

    comments_df = match_xml_and_database.merge_comments_and_metadata(loc_project_metadata=project_metadata,
                                                                     loc_comments_df = comments_df)

    comments_df["comment_before_deadline"] = np.where(comments_df["utcTime"] <= comments_df["utc_deadline"], True, False)
    comments_before_deadline = comments_df[comments_df["comment_before_deadline"]]
    comments_after_deadline = comments_df[comments_df["comment_before_deadline"] == False]

    # section 2) calculate quantitative comment statistics TODO: ALSO INSERT COMMENT QUANTITY (not a function, but from SQL statement)
    print(comments_before_deadline.head())

    reply_ratio = comments_before_deadline.groupby("projectID").apply(calculate_comment_statistics.calculate_reply_ratio)
    reply_speed = comments_before_deadline.groupby("projectID").apply(calculate_comment_statistics.calculate_reply_speed)
    reply_length = comments_before_deadline.groupby("projectID").apply(calculate_comment_statistics.calculate_reply_length)

    comment_length = comments_before_deadline.groupby("projectID").apply(calculate_comment_statistics.calculate_comment_length)
    comment_sentiment = comments_before_deadline.groupby("projectID").apply(calculate_comment_statistics.calculate_comment_sentiment)
    
    print(reply_speed.head(100))

    # section 3) positive psychological capital language of creators
    psycap_statistics = calculate_psycap.wrapper_wordcount(comments_before_deadline)

    # section 4) merge all comment statistics into one dataframe
    comment_statistics_df = pd.concat([reply_ratio.rename("reply_ratio"), 
                                       reply_speed.rename("reply_speed"),
                                       reply_length.rename("reply_length"),
                                       comment_length.rename("comment_length"),
                                       comment_sentiment.rename("comment_sentiment")], 
                                      axis=1)
    
    comment_statistics_df = comment_statistics_df.merge(psycap_statistics,
                                                        left_index=True,
                                                        right_index=True)

    # TOPIC MODELING -----------------------
    # section 1) clean, lemmatize, etc. of data
    # TODO

    # section 2) creation of the topics
    # TODO

    # section 3) saving of topics in dataframe columns that describe the projects, for each projectID one :)
    # TODO

    # SAVE DATA ----------------------------
    # TODO (maybe also choose a subsample for the further analysis and prepare manual coding if necessary)
