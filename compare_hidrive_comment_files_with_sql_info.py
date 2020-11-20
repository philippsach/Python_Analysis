import os, os.path
import pymysql
from sqlalchemy import create_engine
import pandas as pd

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
     "category": comment_path_names,
     "downloaded_comment_files": comment_file_count
     }
    )

# merge with amount of comments per category