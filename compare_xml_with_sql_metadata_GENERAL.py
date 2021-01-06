import os, os.path
from sqlalchemy import create_engine
import pandas as pd
import re
import math

# user setting whether all categories should be compared - when True only compares technology
bol_technology_mac_comparison = True

parent_directory = "/Users/philippsach/HiDrive/public/Kickstarter_Data"
save_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/overview_files/"

sqlEngine = create_engine("mysql+pymysql://phil_sach:entthesis2020@85.214.204.221/thesis")

category_data = {
    "category": ["art", "comic", "craft", "dance", "design",
                 "fashion", "food", "games", "journalism", "music",
                 "photo", "publishing", "technology", "theater", "video"]
}

category_df = pd.DataFrame(category_data)

category_df["path"] = category_df.apply(
    lambda x: "/Users/philippsach/HiDrive/public/Kickstarter_Data/" + 
    x["category"] + 
    "/Comments", 
    axis = 1)

category_df["query"] = category_df.apply(
    lambda x: "SELECT Project_Nr, comments, withdrawn_comments, Link, source_laptop, reward_comment_xml FROM combined_metadata WHERE category='" + 
    x["category"] + "'", axis=1)


def getSize(filename):
    st = os.stat(filename)
    return st.st_size


# the following projects need rescraping
def needRescraping(dataframe):
    bol_rescraping = False
    # projects with comments in metadata but file size 37: means no comments in xml file
    if (dataframe["comments"] > 0 and dataframe["filesize"] == 37 and dataframe["comments"] != dataframe["withdrawn_comments"]):
        bol_rescraping = True
    # projects with more than 25 comments - before webscraper did not click show more
    elif (dataframe["comments"] > 25):
        bol_rescraping = True
    # projects for which there is not even a corresponding xml file
    elif (dataframe["comments"] > 0 and math.isnan(dataframe["filesize"]) and dataframe["comments"] != dataframe["withdrawn_comments"]):
        bol_rescraping = True

    return bol_rescraping
    
if __name__ == "__main__":

    # only do complete one if needed:
    if bol_technology_mac_comparison:
        category_df = category_df[category_df["category"] == "technology"]

    for row in category_df.itertuples(index=True, name="cat"):
            print(row.category)

            metadata = pd.read_sql(row.query, con=sqlEngine)
            metadata = metadata.sort_values(by="comments", ascending=False)

            xml_list = os.listdir(row.path)
            xml_df = pd.DataFrame(xml_list, columns=["filename"])

            os.chdir(row.path)

            xml_df["filesize"] = xml_df["filename"].apply(getSize)
            xml_df["Project_Nr"] = xml_df["filename"].apply(
                lambda x: re.search(pattern="comment_(.*?).xml", string=x).group(1))

            metadata = metadata.merge(xml_df, on="Project_Nr", how="left")
            metadata["bolScrapeAgain"] = metadata.apply(needRescraping, axis=1)

            # filter only to projects that still need to be scraped
            metadata = metadata[metadata["bolScrapeAgain"]]

            # add columns that are filled later with information from success status from crawling comments
            metadata["downloaded"] = False
            metadata["error_description"] = ""
            metadata["withdrawn_comments_new"] = ""

            # calculate path where file should be saved
            where_to_save = save_path + row.category + "_metadata.csv"

            # save file
            metadata.to_csv(where_to_save, index=False)


