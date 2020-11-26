import os, os.path
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import re
import math

parent_directory = "/Users/philippsach/HiDrive/public/Kickstarter_Data"

sqlEngine = create_engine("mysql+pymysql://phil_sach:entthesis2020@85.214.204.221/thesis")

query = """
        SELECT category, COUNT(*)
        FROM combined_metadata
        GROUP BY category
        """

combined_metadata = pd.read_sql(query, con = sqlEngine)

path_name = []
file_count = []

for r, d, files in os.walk(parent_directory):
    path_name.append(r)
    file_count.append(len(files))

# only retrieve the paths that contain comments
comment_indices = [i for i, s in enumerate(path_name) if "Comment" in s]

comment_path_names = [path_name[i] for i in comment_indices]
comment_file_count = [file_count[i] for i in comment_indices]

# put together in a pandas dataframe
downloaded_comments_per_category = pd.DataFrame(
    {
     "raw_path": comment_path_names,
     "downloaded_comment_files": comment_file_count
     }
    )

# simplify naming of these category paths
downloaded_comments_per_category[["1", "2", "3", "4", "5", "6", "category", "8"]] = downloaded_comments_per_category.raw_path.str.split("/", expand = True)
downloaded_comments_per_category = downloaded_comments_per_category[["category", "downloaded_comment_files"]]

# merge with amount of files that we should have according to mysql combined_metadata

comparison_metadata_comments_download = combined_metadata.merge(downloaded_comments_per_category, on="category", how="left")

# only analyze all the xml documents for category art right now

sqlEngine = create_engine("mysql+pymysql://phil_sach:entthesis2020@85.214.204.221/thesis")

query = """
        SELECT Project_Nr, comments, withdrawn_comments, Link, source_laptop, reward_comment_xml
        FROM art_copy
        """

art_metadata = pd.read_sql(query, con = sqlEngine)

art_metadata = art_metadata.sort_values(by="comments", ascending = False)

print(art_metadata.dtypes)

print(art_metadata["source_laptop"].unique())
print(art_metadata["reward_comment_xml"].unique())



art_metadata_source_laptop_filtered = art_metadata[art_metadata["source_laptop"] == "guest"]
art_metadata_reward_comment_xml_filtered = art_metadata[art_metadata["reward_comment_xml"] == "False"]

art_path = "/Users/philippsach/HiDrive/public/Kickstarter_Data/art/Comments"

art_xml_list = os.listdir(art_path)
art_xml_df = pd.DataFrame(art_xml_list, columns=["filename"])
os.chdir(art_path)

def getSize(filename):
    st = os.stat(filename)
    return st.st_size

# the following projects need rescraping
def needRescraping(dataframe):
    bol_rescraping = False
    # projects with comments in metadata but file size 37: means no comments in xml file
    if (dataframe["comments"]>0 and dataframe["filesize"]==37):
        bol_rescraping = True
    # projects with more than 25 comments - before webscraper did not click show more
    elif (dataframe["comments"] > 25):
        bol_rescraping = True
    # projects for which there is not even a corresponding xml file
    elif(dataframe["comments"] > 0 and math.isnan(dataframe["filesize"])):
        bol_rescraping = True
    
    return bol_rescraping
        

art_xml_df["filesize"] = art_xml_df["filename"].apply(getSize)
art_xml_df["Project_Nr"] = art_xml_df["filename"].apply(lambda x: re.search(pattern= "comment_(.*?).xml", string = x).group(1))

art_metadata_new = art_metadata.merge(art_xml_df, on="Project_Nr", how="left")
art_metadata_new["bolScrapeAgain"] = art_metadata_new.apply(needRescraping, axis=1)

art_projects_scrape_again = art_metadata_new[art_metadata_new["bolScrapeAgain"]]
art_projects_scrape_again["downloaded"] = False
art_projects_scrape_again["error_description"] = ""
art_projects_scrape_again["withdrawn_comments_new"] = ""

# save art dataframe
art_projects_scrape_again.to_csv("/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML Info/art_xml_tobescraped.csv", Index = False)
