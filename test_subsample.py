#%% import the necessary libraries
import re
import os
import pymysql
import pandas as pd
import seaborn as sns
import plotly.express as px
import plotly.figure_factory as ff
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.model_selection import train_test_split
from scipy import stats
from currency_converter import CurrencyConverter
from datetime import date

from compare_xml_with_sql_metadata_GENERAL import getSize


#%% user settings
status_list = ["Successful", "FAILED"]

# only use design and technology projects (follows Mollick 2014 argumentation, also Calic Mosakowski partly, and Kickstarter groups design+tech together)
category_list = ["design", "technology"]

# set the columns for which the one-sample t-test (two-sided) should be done - comparing subsample with overlying population
columns_ttest_list = ["Duration", "Backers", "number_rewards", "Images", "videos", "goal_amount_dollar", "pledged_amount_dollar", "int_state", "comments", "updates"]

# set the columns that are needed for crawling later
columns_crawl_list = ["category", "Project_Nr", "Link", "updates"]

# set the columns that should be checked for outliers and outliers should be eliminated
columns_outlier_detection_list = ["pledged_amount_dollar", "updates", "comments"]  # goal amount we probably cannot use, because we already set manually between 5000 and 1000000

# set the maximum z value that we accept in outlier detection
use_outlier_detection = False
max_z_outlier = 30

# set the base path where the updates should be stored later
base_save_path = "/Users/philippsach/HiDrive/public/Kickstarter_Data"

# path where the overview file should be store
overview_save_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Python_Analysis/data/overview_files"

# set amount of projects that should be analysed
n_analyse_projects = 1000  

# set random state for reproducibility
random_state = 4

# create dictionary that tells us how to replace the currency signs with letters
'''
short comment: 
54438 of 269644 projects are in another currency than US dollar
for the moment, we do not exclude all the rest that is not in USD 
but could be that we delete the rest later
'''
currency_dict = {
    "€": "EUR",    # euro
    "$": "USD",    # us dollar
    "£": "GBP",    # british pound
    "NZ$": "NZD",  # new zealand dollar
    "HK$": "HKD",  # hongkong dollar
    "kr": "NOK",   # norwegian krones
    "CA$": "CAD",  # canadian dollar
    "DKK": "DKK",  # danish krone
    "NOK": "NOK",  # norwegian krones, again
    "¥": "JPY",    # japanese yen
    "MX$": "MXN",  # mexican peso
    "AU$": "AUD",  # australian dollar
    "Fr": "CHF",   # swiss franc
    "SEK": "SEK",  # swedish krona
    "S$": "SGD",   # singapore dollar
    "CHF": "CHF"   # swiss franc
    }

# list of US states that need to be replaced by "USA" in country column
state_list = ["AL", "AK", "AZ", "AR", "CA", 
              "CO", "CT", "DE", "FL", "GA",
              "HI", "ID", "IL", "IN", "IA",
              "KS", "KY", "LA", "ME", "MD",
              "MA", "MI", "MN", "MS", "MO",
              "MT", "NE", "NV", "NH", "NJ",
              "NM", "NY", "NC", "ND", "OH",
              "OK", "OR", "PA", "RI", "SC",
              "SD", "TN", "TX", "UT", "VT",
              "VA", "WA", "WV", "WI", "WY",
              "DC"  # washington dc. also to be included
              ]


#%% create sql engine and retrieve the data from SQL
sqlEngine = create_engine("mysql+pymysql://phil_sach:entthesis2020@85.214.204.221/thesis")

query = """
        SELECT category, Project_Nr, county_origin, country_origin, state, updates, comments, Link, Duration, Backers, Goal, Pledged, Images, videos, number_rewards, end_year, end_month, end_day
        FROM combined_metadata
        """

original_combined_metadata = pd.read_sql(query, con = sqlEngine)


#%% clean the data
# funding goal needs currency to be extracted

# separate currency and amount for goal, clean from unneccessary thousands separator
original_combined_metadata["goal_currency"] = original_combined_metadata["Goal"].str.extract(pat='(^\D+)')
original_combined_metadata["goal_currency"] = original_combined_metadata["goal_currency"].str.rstrip()
original_combined_metadata["goal_amount"] = original_combined_metadata["Goal"].str.extract(pat='(\d.*)')
original_combined_metadata["goal_amount"] = original_combined_metadata["goal_amount"].map(lambda x: float(x.replace(",", "")))

# similar for pledged: separate currency and amount
original_combined_metadata["pledged_currency"] = original_combined_metadata["Pledged"].str.extract(pat='(^\D+)')
original_combined_metadata["pledged_currency"] = original_combined_metadata["pledged_currency"].str.rstrip()
original_combined_metadata["pledged_amount"] = original_combined_metadata["Pledged"].str.extract(pat='(\d.*)')
original_combined_metadata["pledged_amount"] = original_combined_metadata["pledged_amount"].map(lambda x: float(x.replace(",", "")))

# change the currency symbols to three letter abbreviations
original_combined_metadata["goal_currency"] = original_combined_metadata["goal_currency"].map(currency_dict)
original_combined_metadata["pledged_currency"] = original_combined_metadata["pledged_currency"].map(currency_dict)

original_combined_metadata["deadline_date"] = original_combined_metadata.apply(lambda x: date(int(x["end_year"]), int(x["end_month"]), int(x["end_day"])) ,axis=1)


# convert the currencies
c = CurrencyConverter(fallback_on_missing_rate=True, fallback_on_missing_rate_method="linear_interpolation")
c.convert(100, "EUR", "USD")
c.convert(100, "USD", "USD")

original_combined_metadata["goal_amount_dollar"] = original_combined_metadata.apply(lambda x: c.convert(x["goal_amount"], 
                                                                                                        x["goal_currency"], 
                                                                                                        "USD", 
                                                                                                        date=x["deadline_date"]), 
                                                                                    axis=1)

original_combined_metadata["pledged_amount_dollar"] = original_combined_metadata.apply(lambda x: c.convert(x["pledged_amount"],
                                                                                                           x["pledged_currency"],
                                                                                                           "USD",
                                                                                                           date=x["deadline_date"]),
                                                                                       axis=1)


# change data type to integer of the following columns
'''
- number_rewards
- Backers
- Images
- videos
'''
original_combined_metadata["number_rewards"] = original_combined_metadata["number_rewards"].astype("int")
original_combined_metadata["Backers"] = original_combined_metadata["Backers"].map(lambda x: int(x.replace(",", "")))
original_combined_metadata["Images"] = original_combined_metadata["Images"].astype("int")
original_combined_metadata["videos"] = original_combined_metadata["videos"].astype("int")

# clean up duration
original_combined_metadata["Duration"] = original_combined_metadata["Duration"].str.extract(pat='(^[0-9]+)')
original_combined_metadata["Duration"] = original_combined_metadata["Duration"].astype("int")

# clean up state names and replace it with NA
original_combined_metadata["country_origin"] = original_combined_metadata["country_origin"].str.rstrip()
original_combined_metadata["country_origin"].replace(to_replace=state_list, value="USA", inplace=True)

# only use projects that either are successful or failed, but not cancelled or still ongoing
original_combined_metadata = original_combined_metadata[original_combined_metadata["state"].isin(status_list)]

# add an artifical 1/0 column about state of project for easier statistics
original_combined_metadata["int_state"] = original_combined_metadata.apply(lambda x: 1 if x["state"] == "Successful" else 0, axis=1)


#%% removal of projects we do not want to use

combined_metadata = original_combined_metadata.copy()

# only use projects from the United States (follows Allison et al 2017)
combined_metadata = combined_metadata[combined_metadata["country_origin"] == "USA"]

# taken from Mollick 2014 - due to unsure seriousness of the projects
combined_metadata = combined_metadata[combined_metadata["goal_amount_dollar"] > 5000]  # reduces approx. by half
combined_metadata = combined_metadata[combined_metadata["goal_amount_dollar"] < 1000000]  # within USA, reduces from 175401 to 174818 projects

# only use projects with at least one update
combined_metadata = combined_metadata[combined_metadata["updates"] > 0]

# only use projects with at least one reward (reduces only from 244227 to 244187 projects)
combined_metadata = combined_metadata[combined_metadata["number_rewards"] > 0]

# combined_metadata = combined_metadata[combined_metadata["pledged_amount_dollar"] < 3000000]  # MAYBE HANDLE THIS BETTER WITH OUTLIER DETECTION using z values

# statistics
# combined_metadata_by_category = combined_metadata.groupby("category").count()

#%% outlier detection
if use_outlier_detection:
    z_columns_list = []
    
    # calculate the z values for the columns which should be used in outlier detection
    for outlier_column in columns_outlier_detection_list:
        z_name = "z_score_" + outlier_column
        combined_metadata[z_name] = stats.zscore(combined_metadata[outlier_column])  # do not use np.abs because we just want to detect outliers to the top, below we already set the 5000 minimum
        z_columns_list.append(z_name)
       
    for z_column in z_columns_list:
       combined_metadata = combined_metadata[combined_metadata[z_column] < max_z_outlier]


#%% calculation of the population means along the variables of interest
population_means = original_combined_metadata.groupby("category").mean()
reduced_population_means = combined_metadata.groupby("category").mean()


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

# see how many projects are here from each category
chosen_metadata_by_category = chosen_metadata.groupby("category").count()
chosen_metadata_by_category_and_state = chosen_metadata.groupby(["category", "state"]).count()

# calculate how many projects are in this current sample of chosen projects
n_projects_sample = chosen_metadata.shape[0]


#%% boxplots of the data
chosen_metadata.boxplot(column="pledged_amount_dollar" ,return_type="axes")

z = pd.Series(np.abs(stats.zscore(chosen_metadata["pledged_amount_dollar"])), name="z_value_pledged_amount_dollar")
sorted_z = z.sort_values(ascending=False)

sorted_chosen_metadata = chosen_metadata.sort_values(by="pledged_amount_dollar", ascending=False)

#%% calculate the new final metadata (300 per category, non stratified)

final_metadata_technology = chosen_metadata[chosen_metadata["category"] == "technology"].sample(n=500, random_state=random_state)
final_metadata_design = chosen_metadata[chosen_metadata["category"] == "design"].sample(n=500, random_state=random_state)


# alternative method of creating the final metadata
number_of_projects_in_category = chosen_metadata.groupby("category").count()["Project_Nr"]


def choose_final_metadata(population_data, category):
    
    category_metadata = population_data[population_data["category"] == category].copy()
    
    population_size = number_of_projects_in_category.loc[category]
    sample_proportion = 300 / population_size
    
    final_metadata, rest_metadata = train_test_split(category_metadata, test_size=1-sample_proportion, random_state=random_state, stratify=category_metadata["int_state"])
    
    return final_metadata

final_metadata_technology = choose_final_metadata(population_data=chosen_metadata, category="technology")
final_metadata_video = choose_final_metadata(population_data=chosen_metadata, category="video")
final_metadata_design = choose_final_metadata(population_data=chosen_metadata, category="design")


#%% one sample t-test for the new chosen data

ttest_df = pd.DataFrame()
ttest_dict = {}

for category in category_list: 
    print("category: ", category)
    
    # choose the right metadata per category
    if category == "technology":
        subsample_metadata = final_metadata_technology
    
    elif category == "design":
        subsample_metadata = final_metadata_design
        
    else:
        raise Exception("Sorry, category not in final category list")
    
    subsample_ttest_results_list = []
    
    for characteristic in columns_ttest_list:
        print("characteristic: ", characteristic)
        subsample_ttest_results_list.append(stats.ttest_1samp(
            a=subsample_metadata[characteristic],
            popmean=reduced_population_means.loc[category, characteristic]
            )[1]  # only choose the p-value to be saved
        )

    ttest_dict[category] = subsample_ttest_results_list 
        
ttest_df = pd.DataFrame.from_dict(ttest_dict, orient="index", columns=columns_ttest_list)


#%% just for reasons to find a good random state - might delete later
test_random_states = list(range(1, 101))

columns_test_states = columns_ttest_list.copy()
columns_test_states.append("random_state")

complete_ttest_df = pd.DataFrame()

for one_random_state in test_random_states:
    test_metadata_technology = chosen_metadata[chosen_metadata["category"] == "technology"].sample(n=500, random_state=one_random_state)
    test_metadata_design = chosen_metadata[chosen_metadata["category"] == "design"].sample(n=500, random_state=one_random_state)
    
    optimize_ttest_df = pd.DataFrame()
    ttest_dict = {}
    
    for category in category_list: 
        print("category: ", category)
    
        # choose the right metadata per category
        if category == "technology":
            subsample_metadata = test_metadata_technology
        
        elif category == "design":
            subsample_metadata = test_metadata_design
            
        else:
            raise Exception("Sorry, category not in final category list")
        
        subsample_ttest_results_list = []
        
        for characteristic in columns_ttest_list:
            print("characteristic: ", characteristic)
            subsample_ttest_results_list.append(stats.ttest_1samp(
                a=subsample_metadata[characteristic],
                popmean=reduced_population_means.loc[category, characteristic]
                )[1]  # only choose the p-value to be saved
            )
            
        subsample_ttest_results_list.append(one_random_state)
    
        ttest_dict[category] = subsample_ttest_results_list 
            
    ttest_df = pd.DataFrame.from_dict(ttest_dict, orient="index", columns=columns_test_states)
    complete_ttest_df = complete_ttest_df.append(ttest_df)


# now make it easier for interpretation: delete all rows with less than 0.05 
interpret_ttest_df = complete_ttest_df[complete_ttest_df > 0.05].dropna()

# split analysis for design and technology
interpret_ttest_df_design = interpret_ttest_df[interpret_ttest_df.index == "design"]
interpret_ttest_df_tech = interpret_ttest_df[interpret_ttest_df.index == "technology"]

# calculate the average over the columns except for the random state column
interpret_ttest_df_design["mean"] = interpret_ttest_df_design.iloc[:, 0:10].mean(axis=1)
interpret_ttest_df_tech["mean"] = interpret_ttest_df_tech.iloc[:, 0:10].mean(axis=1)

interpret_ttest_df_design = interpret_ttest_df_design.sort_values(by="mean", ascending=False)
interpret_ttest_df_tech = interpret_ttest_df_tech.sort_values(by="mean", ascending=False)

#%% visualize distribution etc. of 
chosen_metadata_technology = chosen_metadata[chosen_metadata["category"] == "technology"]
chosen_metadata_design = chosen_metadata[chosen_metadata["category"] == "design"]
chosen_metadata_video = chosen_metadata[chosen_metadata["category"] == "video"]


plt.figure()
fig1 = sns.kdeplot(final_metadata_technology["goal_amount_dollar"])
plt.figure()
fig2 = sns.kdeplot(chosen_metadata_technology["goal_amount_dollar"])

# do the same in plotly
fig = ff.create_distplot(final_metadata_technology["goal_amount_dollar"], group_labels = ["technology"], show_hist=False)

#%% calculate the old final metadata (stratified by category)

# calculate the proportion of the "train" dataset that we will use
'''
will use train test split of scikit learn to choose the projects
because it provides easy functionality for stratified sampling;
need the proportion of the projects we want to analyse of all the projects that are in chosen_metadata by now
'''
sample_proportion = n_analyse_projects / n_projects_sample

final_metadata, rest_metadata = train_test_split(chosen_metadata, 
                                                 test_size=(1-sample_proportion),
                                                 random_state=random_state,
                                                 stratify=chosen_metadata[["category", "state"]]  # old: also used state for stratification
                                                 )

final_metadata_by_category = final_metadata.groupby("category").count()
final_metadata_by_category_and_state = final_metadata.groupby(["category", "state"]).count()

final_success_statistics = final_metadata.groupby("category")["int_state"].mean()

final_update_statistics_by_category = final_metadata.groupby("category")["updates"].describe()
final_update_statistics_by_category_and_state = final_metadata.groupby(["category", "state"])["updates"].describe()
final_comment_statistics_by_category = final_metadata.groupby("category")["comments"].describe()
final_comment_statistics_by_category_and_state = final_metadata.groupby(["category", "state"])["comments"].describe()


#%% prepare the one-sample t-test to compare statistics within chosen sample with overlying population

'''
null hypothesis H_0 is: the expected value (mean) of the subsample is equal to the given population mean, popmean
we reject the null hypothesis if p is < 0.05
otherwise, we accept the null hypothesis
'''

# create empty data frame for the 
ttest_df = pd.DataFrame()
ttest_dict = {}

for category in category_list:
    print("category: ", category)
    
    # filter to data for this category
    subsample_metadata = final_metadata[final_metadata["category"] == category].copy()
    
    subsample_ttest_results_list = []
    
    for characteristic in columns_ttest_list:
        print("characteristic: ", characteristic)
        subsample_ttest_results_list.append(stats.ttest_1samp(
            a=subsample_metadata[characteristic],
            popmean=reduced_population_means.loc[category, characteristic]
            )[1]  # only choose the p-value to be saved
        )

    ttest_dict[category] = subsample_ttest_results_list 
        
ttest_df = pd.DataFrame.from_dict(ttest_dict, orient="index", columns=columns_ttest_list)


#%% validation of the results of the dataframe we just produced

# test on one subcategory: technology
#final_metadata_technology = final_metadata[final_metadata["category"] == "technology"]

print(stats.ttest_1samp(a=final_metadata_technology["Duration"], popmean=population_means.loc["technology", "Duration"]))

# calculate the means on the subsamples so that we can also manually compare between population value and value for subsample

subsample_means = final_metadata_technology.mean()
#subsample_means = final_metadata.groupby("category").mean()


# just create these for faster visual comparison in Notion
important_population_means = population_means[population_means.index.isin(category_list)]
important_population_means = important_population_means[columns_ttest_list]
important_reduced_population_means = reduced_population_means[reduced_population_means.index.isin(category_list)]
important_reduced_population_means = important_reduced_population_means[columns_ttest_list]
important_subsample_means = subsample_means[columns_ttest_list]


#%% create the needed overview files about the updates

final_metadata_updates = final_metadata.copy()
final_metadata_updates = final_metadata_updates[columns_crawl_list]
final_metadata_updates["save_path"] = final_metadata_updates.apply(
    lambda x: os.path.join(base_save_path, x["category"], "Updates"), axis=1)

final_metadata_updates["faq"] = np.nan
final_metadata_updates["updates_only_visible_for_backers"] = np.nan

final_metadata_updates["updates_downloaded"] = False


#%% save the needed files in corresponding locations on the hard disk
final_metadata_updates.to_csv(os.path.join(overview_save_path, "update_metadata.csv"), index=False)

#%% archive for comments metadata creation - not needed anymore...
final_metadata_comments = final_metadata.copy()

final_metadata_comments["comments_path"] = final_metadata_comments.apply(
    lambda x: "/Users/philippsach/HiDrive/public/Kickstarter_Data/" + 
    x["category"] + "/Comments", 
    axis=1)

''' 
THIS IS JUST COPIED YET FOR REFERENCE; NEEDS FURTHER ADAPTION AND THINKING HOW TO DO IT THEN 
with the chosen projects
- check if the comments need to be rescraped
- then download the missing comments

- create the metafile for the updates as no updates have been downloaded yet
- adapt the crawl_updates script so that it works together with the metadata
- adapt crawl_updates to try to avoid detection of CRAWLING ACTIVITY!!!! 
'''

xml_df = pd.DataFrame()

# TODO: NEED TO MOVE ALL THE DOWNLOADED COMMENTS FIRST TO THE DIRECTORY

for category in final_metadata_comments["category"].unique():
    print(category)
    comments_path = "/Users/philippsach/HiDrive/public/Kickstarter_Data/" + category + "/Comments"
    print(comments_path)
    
    cat_xml_list = os.listdir(comments_path)
    cat_xml_df = pd.DataFrame(cat_xml_list, columns=["filename"])
    cat_xml_df["category"] = category
    
    # change directory to the specific path
    os.chdir(comments_path)
    cat_xml_df["filesize"] = cat_xml_df["filename"].apply(getSize)
    
    # add the project number extracted from file name
    cat_xml_df["Project_Nr"] = cat_xml_df["filename"].apply(
        lambda x: re.search(pattern="comment_(.*?).xml", string=x).group(1))
    
    # create dataframe containing info from hard disk including all categories
    xml_df = xml_df.append(cat_xml_df)

for row in final_metadata_comments.itertuples(index=True, name="cat"):
    
    


#%% further stuff (more of archive)

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
