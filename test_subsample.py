#%% import the necessary libraries
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.model_selection import train_test_split


#%% user settings
status_list = ["Successful", "FAILED"]

# the top 2 categories with the most average comments are chosen
category_list = ["design", "technology"]  # could think of also using games in the future

random_state = 42

#%% create sql engine and retrieve the data from SQL
sqlEngine = create_engine("mysql+pymysql://phil_sach:entthesis2020@85.214.204.221/thesis")

query = """
        SELECT category, Project_Nr, state, Goal, Pledged, updates, comments
        FROM combined_metadata
        """

original_combined_metadata = pd.read_sql(query, con = sqlEngine)


#%% removal of projects we do not want to use
# only use projects that either are successful or failed, but not cancelled or still ongoing
original_combined_metadata = original_combined_metadata[original_combined_metadata["state"].isin(status_list)]

# add an artifical 1/0 column about state of project for easier statistics
original_combined_metadata["int_state"] = original_combined_metadata.apply(lambda x: 1 if x["state"] == "Successful" else 0, axis=1)

combined_metadata = original_combined_metadata.copy()

# only use projects with at least one comment 
combined_metadata = combined_metadata[combined_metadata["comments"] > 0]

# only use projects with at least one update
combined_metadata = combined_metadata[combined_metadata["updates"] > 0]


#%% analysis on the data
# check how many project numbers are unique 
print(original_combined_metadata["Project_Nr"].nunique())

original_category_statistics = original_combined_metadata.groupby("category").count().sort_values(by="Project_Nr", ascending=False)
original_category_statistics_by_state = original_combined_metadata[original_combined_metadata["state"]=="Successful"].groupby(["category", "state"]).count().sort_values(by="Project_Nr", ascending=False)


original_comment_statistics = original_combined_metadata["comments"].describe()
original_comment_statistics_by_category = original_combined_metadata.groupby("category")["comments"].describe().sort_values(by="mean", ascending=False)
original_comment_statistics_by_category_and_state = original_combined_metadata.groupby(["category", "state"])["comments"].describe().sort_values(by="mean", ascending=False)

original_update_statistics = original_combined_metadata["updates"].describe()
original_update_statistics_by_category = original_combined_metadata.groupby("category")["updates"].describe().sort_values(by="mean", ascending=False)
original_update_statistics_by_category_and_state = original_combined_metadata.groupby(["category", "state"])["updates"].describe().sort_values(by="mean", ascending=False)

original_success_statistics = original_combined_metadata["int_state"].mean()
original_success_statistics_by_category = original_combined_metadata.groupby("category")["int_state"].mean().sort_values(ascending=False)
# original_test = original_combined_metadata.groupby(["category", "state"]).count()

comment_statistics = combined_metadata["comments"].describe()
comment_statistics_by_category = combined_metadata.groupby("category")["comments"].describe().sort_values(by="mean", ascending=False)
comment_statistics_by_category_and_state = combined_metadata.groupby(["category", "state"])["comments"].describe().sort_values(by="mean", ascending=False)

update_statistics = combined_metadata["updates"].describe()
update_statistics_by_category = combined_metadata.groupby("category")["updates"].describe().sort_values(by="mean", ascending=False)
update_statistics_by_category_and_state = combined_metadata.groupby(["category", "state"])["updates"].describe().sort_values(by="mean", ascending=False)

success_statistics = combined_metadata["int_state"].mean()
success_statistics_by_category = combined_metadata.groupby("category")["int_state"].mean().sort_values(ascending=False)

#%% stratified subsampling of XXX projects we want to analyse

chosen_metadata = combined_metadata.copy()

# only use projects of our categories that should be chosen
chosen_metadata = chosen_metadata[chosen_metadata["category"].isin(category_list)]


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
