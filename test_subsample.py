import pymysql
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.model_selection import train_test_split

sqlEngine = create_engine("mysql+pymysql://phil_sach:entthesis2020@85.214.204.221/thesis")

query = """
        SELECT Project_Nr, country_origin, Goal, Pledged, updates, comments, state, category 
        FROM combined_metadata
        WHERE comments > 0
        """

status_list = ["Successful", "FAILED"]


combined_metadata = pd.read_sql(query, con = sqlEngine)

print(combined_metadata["Project_Nr"].nunique())

combined_metadata = combined_metadata[combined_metadata["state"].isin(status_list)]
combined_metadata["comments"] = combined_metadata["comments"].replace(",", "")

print(combined_metadata[combined_metadata["comments"]] == "1534")

combined_metatdata["comments"] = combined_metadata["comments"].astype("int64")

duplicates = combined_metadata[combined_metadata.duplicated(subset=["Project_Nr"])]
duplicates = duplicates.sort_values(by = "Project_Nr", ascending = True)

duplicates_project_nr_and_category = combined_metadata[combined_metadata.duplicated(subset = ["Project_Nr", "category"])]


# replace values in column category with numerical values so that we can carry out numerical analysis
dict_category = {
    "art": 1,
    "comic": 2,
    "craft": 3,
    "dance": 4,
    "design": 5,
    "fashion": 6,
    "food": 7,
    "games": 8,
    "journalism": 9,
    "music": 10,
    "photo": 11,
    "publishing": 12,
    "technology": 13,
    "theater": 14,
    "video": 15
}

combined_metadata_numerical = combined_metadata.replace({"category": dict_category})

print(combined_metadata_numerical["category"].unique())


train, test = train_test_split(combined_metadata, test_size = 0.99, random_state=42, stratify=combined_metadata[["category", "state"]])

train_stats_category = train.groupby("category")["Project_Nr"].count().reset_index()
train_stats_category["percentage"] = train_stats_category["Project_Nr"]/train_stats_category["Project_Nr"].sum()

train_stats_successful = train.groupby("state")["Project_Nr"].count().reset_index()
train_stats_successful["percentage"] = train_stats_successful["Project_Nr"]/train_stats_successful["Project_Nr"].sum()



original_stats_category = combined_metadata.groupby("category")["Project_Nr"].count().reset_index()
original_stats_category["percentage"] = original_stats_category["Project_Nr"]/original_stats_category["Project_Nr"].sum()

original_stats_successful = combined_metadata.groupby("state")["Project_Nr"].count().reset_index()
original_stats_successful["percentage"] = original_stats_successful["Project_Nr"]/original_stats_successful["Project_Nr"].sum()

print("datatypes of combined_metadata: ", combined_metadata.dtypes)

print("average number of comments in original set: ", combined_metadata["comments"].mean())
