
#%% import packages
import pandas as pd
import os
import datetime as dt

from sqlalchemy import create_engine

import calculate_psycap

#%% user settings
maxqda_file_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/00_FINAL/exports_maxqda"
coding_file_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Austausch Daniel Philipp/datasets for coding"
overview_file_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Python_Analysis/data/overview_files"
statistics_file_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/00_FINAL"
    
save_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/00_FINAL"

sqlEngine = create_engine("mysql+pymysql://phil_sach:entthesis2020@85.214.204.221/thesis")
query = "SELECT category, Project_Nr, creator_experience FROM combined_metadata"


#%% function definition

def multiindex_pivot(df, index=None, columns=None, values=None):
    if index is None:
        names = list(df.index.names)
        df = df.reset_index()
    else:
        names = index
    
    list_index = df[names].values
    tuples_index = [tuple(i) for i in list_index]
    
    df = df.assign(tuplex_index=tuples_index)
    df = df.pivot(index="tuples_index", columns=columns, values=values)
    tuples_index = df.index
    index = pd.MultiIndex.from_tuples(tuples_index, names=names)
    df.index = index
    
    return df
    

def create_long_raw_data_codeabdeckung(codeabdeckung):
    
    # remove the total column
    codeabdeckung = codeabdeckung.drop("TOTAL", axis=1)
    
    codeabdeckung = codeabdeckung.set_index(["Codesystem"])
    
    # delete rows that only contain nan values
    codeabdeckung = codeabdeckung.dropna(how="all")
    
    transposed = codeabdeckung.transpose()
    transposed = transposed.reset_index().rename({"index": "project_updateID"}, axis=1)
    
    transposed[["Project_Nr", "Update_Nr"]] = transposed["project_updateID"].str.rsplit("_", n=1, expand=True)
    
    transposed["NICHT CODIERT"] = transposed["NICHT CODIERT"].map(lambda x: x.replace(".", ""))
    transposed["CODIERT"] = transposed["CODIERT"].map(lambda x: x.replace(".", ""))
    transposed["GESAMTTEXT"] = transposed["GESAMTTEXT"].map(lambda x: x.replace(".", ""))
    
    transposed[["CODIERT", "CODIERTTEXT"]] = transposed["CODIERT"].str.split("(", expand=True)
    
    transposed["CODIERTTEXT"] = transposed["CODIERTTEXT"].str.replace(")", "")
    
    transposed.loc[:, "content": "Press Coverage (Articles, Kickbooster, etc.)"] = transposed.loc[:, "content": "Press Coverage (Articles, Kickbooster, etc.)"].replace(to_replace=",", value=".", regex=True)
    
    transposed = transposed.fillna("0")
    
    transposed["category"] = "technology"
    
    transposed = transposed[[
        "category",
        "project_updateID",
        "Project_Nr",
        "Update_Nr",
        "GESAMTTEXT",
        "CODIERTTEXT",
        "NICHT CODIERT",
        "content",
        "Product",
        "Funding",
        "Team (Prior Experience,New Members,...)",
        "Business",
        "External Certification (Awards,...)",
        "Stretch Goal Achievement",
        "Stretch Goal Announcement",
        "Rewards added",
        "Links / Advertisement to other projects",
        "Reminder",
        "Crowd Appreciation",
        "Status Update",
        "Referral Program",
        "Offerings (Discounts,  Cashbacks, etc.)",
        "Ask for Active Participation (Survey, Share with friends ...)",
        "FAQ (Addition,  Implementation,  Auf Fragen eingegangen ...)",
        "Press Coverage (Articles, Kickbooster, etc.)"
        ]]
    
    transposed.loc[:, "content": ] = transposed.loc[:, "content":].apply(lambda x: x.str.strip("%"))
    transposed.loc[:, "GESAMTTEXT": ] = transposed.loc[:, "GESAMTTEXT": ].astype("float")
    transposed.loc[:, "content": ] = transposed.loc[:, "content": ] / 100
        
    long_raw_data = transposed.melt(
        id_vars=["category", "project_updateID", "Project_Nr", "Update_Nr", "GESAMTTEXT", "CODIERTTEXT", "NICHT CODIERT", "content"],
        value_vars=[
        "Funding",
        "Team (Prior Experience,New Members,...)",
        "Business",
        "External Certification (Awards,...)",
        "Stretch Goal Achievement",
        "Stretch Goal Announcement",
        "Rewards added",
        "Links / Advertisement to other projects",
        "Reminder",
        "Crowd Appreciation",
        "Status Update",
        "Referral Program",
        "Offerings (Discounts,  Cashbacks, etc.)",
        "Ask for Active Participation (Survey, Share with friends ...)",
        "FAQ (Addition,  Implementation,  Auf Fragen eingegangen ...)",
        "Press Coverage (Articles, Kickbooster, etc.)"
        ])
    
    return long_raw_data

#%% coding

# IMPORT FILES FROM MAXQDA
codeabdeckung_tech = pd.read_excel(os.path.join(maxqda_file_path, "2021-03-03_Codeabdeckung_Technology_Content.xlsx"))
codeabdeckung_design = pd.read_excel(os.path.join(maxqda_file_path, "2021-03-11_Codeabdeckung_Design_Content.xlsx"))

# GET creator experience
creator_experience = pd.read_sql(query, con=sqlEngine)
creator_experience["creator_experience"] = creator_experience["creator_experience"].str.replace("\n+", "",regex=True)  # new line characters
creator_experience["creator_experience"] = creator_experience["creator_experience"].str.replace(",", "",regex=True)  # comma in number

creator_experience["creator_experience"] = creator_experience["creator_experience"].str.strip() # remove leading and trailing whitespaces
creator_experience["creator_experience"] = creator_experience["creator_experience"].str.replace("First created", "0", regex=True)  # new line characters
creator_experience.loc[creator_experience["creator_experience"].str.contains("backed"), "creator_experience"] = "0"

creator_experience["creator_experience"] = creator_experience["creator_experience"].str.split().str[0]
creator_experience["creator_experience"] = creator_experience["creator_experience"].astype(int)

# TODO: INSERT HERE - importing of codeabdeckung for the content related title and processing of it!! 

long_raw_data_tech = create_long_raw_data_codeabdeckung(codeabdeckung_tech)
long_raw_data_design = create_long_raw_data_codeabdeckung(codeabdeckung_design)

# union tech and design long raw data
long_raw_data = long_raw_data_tech.append(long_raw_data_design)

# BRING TOGETHER WITH INFORMATION UPDATES THEMSELVES
coding_tech = pd.read_excel(os.path.join(coding_file_path, "coding_updates_tech.xlsx"))
coding_design = pd.read_excel(os.path.join(coding_file_path, "coding_updates_design.xlsx"))

coding_df = coding_tech.append(coding_design)

coding_df["name_weekday"] = coding_df["time"].dt.day_name()
coding_df["bol_weekend"] = coding_df["time"].dt.dayofweek
coding_df["bol_weekend"] = coding_df["bol_weekend"].apply(lambda x: 1 if x>=5 else 0)

coding_df["time"] = coding_df["time"].dt.date
coding_df["deadline_date"] = coding_df["deadline_date"].dt.date



coding_df = coding_df.iloc[:, 1:]

coding_df[["Project_Nr", "Update_Nr"]] = coding_df["project_updateID"].str.rsplit("_", n=1, expand=True)

coding_df = coding_df.drop(["original_link"], axis=1)

# TODO: INSERT HERE: CLEAN CONTENT OF UPDATES

coding_df["content"] = coding_df["content"].str.replace("\S*@\S*\s?", "", regex=True)  # email-addresses
coding_df["content"] = coding_df["content"].str.replace("\n+", " ",regex=True)  # new line characters
coding_df["content"] = coding_df["content"].str.replace("(-|_){2,}", "", regex=True)  # the -- or __ when they divide projects
coding_df["content"] = coding_df["content"].str.replace("( ){2,}", " ", regex=True)  # replace two or more white spaces with only one whitespace
coding_df["content"] = coding_df["content"].str.replace("\'", "", regex=True)  # distracting single quotes
coding_df["content"] = coding_df["content"].str.replace(
    r"(http(s)?:\/\/.)?(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)"
    , "", regex=True)  # urls


coding_df = calculate_psycap.count_words_in_list(coding_df)

# change types of counted columns
coding_df.loc[:, "count_organizational_optimism" : "word_count"] = coding_df.loc[:, "count_organizational_optimism" : "word_count"].astype(float)

# calculate the share of prosocial words
coding_df["share_prosocial"] = coding_df["count_prosocial"] / coding_df["word_count"]


# re-establish a category column
coding_df["category"] = coding_df["group_category"].copy()
coding_df["category"] = coding_df["category"].replace(
    {"technology_part2": "technology",
     "technology_part1": "technology",
     "design_part2": "design",
     "design_part1": "design"}
    )

coding_third_info_df = coding_df[["category", "project_updateID", "first_seven_days", "last_seven_days"]]
coding_weekdays_df = coding_df[["category", "project_updateID", "name_weekday", "bol_weekend"]]


# merge the two dataframes
update_level_data = long_raw_data.merge(coding_third_info_df, how="left", on=["category", "project_updateID"])
update_level_data["third"] = update_level_data.apply(lambda x: 1 if x["first_seven_days"] else 0, axis=1)
update_level_data["third"] = update_level_data.apply(lambda x: 3 if x["last_seven_days"] else x["third"], axis=1)
update_level_data["third"] = update_level_data.apply(lambda x: 2 if x["third"] == 0 else x["third"], axis=1)

# create column for pivoting into wide format
update_level_data_pivoting_raw = update_level_data.copy()
update_level_data_pivoting_raw["pivoting_column"] = update_level_data_pivoting_raw["Codesystem"] + "_" + update_level_data_pivoting_raw["third"].astype(str)


# Section 1) PER TIME FRAME WITHIN PROJECT (T1, T2, T3)
update_level_data_pivoting = update_level_data_pivoting_raw[["project_updateID", "pivoting_column", "value"]]
update_level_data_wide = update_level_data_pivoting.pivot(values="value", columns="pivoting_column", index="project_updateID")
update_level_data_wide = update_level_data_wide.reset_index()

update_level_data_wide = update_level_data_wide.fillna(0)
update_level_data_wide[["Project_Nr", "Update_Nr"]] = update_level_data_wide["project_updateID"].str.rsplit("_", n=1, expand=True)


# Section 2) Update level data without the t1, t2, t3
update_level_data = update_level_data.pivot(values="value", columns="Codesystem", index="project_updateID")
update_level_data = update_level_data.reset_index()
update_level_data = update_level_data.fillna(0)

update_level_data = update_level_data.merge(coding_df, how="left", on="project_updateID")
update_level_data["third"] = update_level_data.apply(lambda x: 1 if x["first_seven_days"] else 0, axis=1)
update_level_data["third"] = update_level_data.apply(lambda x: 3 if x["last_seven_days"] else x["third"], axis=1)
update_level_data["third"] = update_level_data.apply(lambda x: 2 if x["third"] == 0 else x["third"], axis=1)

# prepare information about updates how many are on weekend 
update_level_data[["Project_Nr", "Update_Nr"]] = update_level_data["project_updateID"].str.rsplit("_", n=1, expand=True)
project_level_weekend = update_level_data[["Project_Nr", "bol_weekend"]].copy()
project_level_weekend = project_level_weekend.groupby("Project_Nr").mean()



# TODO: HERE OR SOMEWHERE ELSE? - GET THE category, project_nr, crowdfunding experience from SQL
# TODO: clean the crowdfunding experience to an integer value

project_level_xml_and_structural_data = update_level_data[["Project_Nr", "picture_count", "gif_count", "video_count", "like_count", "comment_count",
                                            "count_organizational_optimism", "count_organizational_hope", "count_organizational_resilience",
                                            "count_organizational_confidence", "count_misspellings", "count_psycap", "count_prosocial",
                                            "share_prosocial", "word_count"]].copy()
project_level_xml_and_structural_data = project_level_xml_and_structural_data.fillna(0)
project_level_xml_and_structural_data = project_level_xml_and_structural_data.groupby("Project_Nr").mean()
project_level_xml__and_structural_data = project_level_xml_and_structural_data.reset_index()

# get the project means for each type of information
project_level_data_wide = update_level_data_wide.groupby("Project_Nr").mean()

# Section 2) PER PROJECT - totals per CATEGORY
project_level_data_totals = update_level_data_pivoting_raw[["Project_Nr", "Codesystem", "value"]]
project_level_data_totals = project_level_data_totals.fillna(0)
project_level_data_totals = project_level_data_totals.groupby(["Project_Nr", "Codesystem"]).mean()
project_level_data_totals = project_level_data_totals.reset_index()

project_level_data_totals = project_level_data_totals.pivot(values="value", columns="Codesystem", index="Project_Nr")

# BRING TOGETHER Totals and per time frame
project_level_data = project_level_data_totals.merge(project_level_data_wide, how="left", on="Project_Nr")
project_level_data = project_level_data.reset_index()

# add information of share of updates on weekends
project_level_data = project_level_data.merge(project_level_weekend, how="left", on="Project_Nr")
project_level_data = project_level_data.merge(project_level_xml_and_structural_data, how="left", on="Project_Nr")



#%% bring together with overview file
overview_file = pd.read_csv(os.path.join(overview_file_path, "current_update_metadata.csv"))

overview_file = overview_file.merge(creator_experience, how="left", on=["category", "Project_Nr"])

statistics_hidden_updates = pd.read_csv(os.path.join(statistics_file_path, "updates_and_hidden_updates_within_duration.csv"))
statistics_hidden_updates = statistics_hidden_updates.rename(columns={"projectID": "Project_Nr"})

overview_file = overview_file.merge(statistics_hidden_updates, how="left", on="Project_Nr")

overview_file["deadline_date"] = pd.to_datetime(overview_file["deadline_date"], format = "%Y-%m-%d")
overview_file["year"] = overview_file["deadline_date"].dt.year
overview_file["deadline_date"] = overview_file["deadline_date"].dt.date


overview_file = overview_file.drop(["Link", "state", "comments", "hidden_project", "save_path", "own_dev_comment", "updates_downloaded", "Images", "videos"],axis=1)
overview_file = overview_file.rename(columns={"int_state":"state", "Duration":"duration", "Backers":"backers"})

project_level_data = project_level_data.merge(overview_file, how="left", on="Project_Nr")
project_level_data = project_level_data.rename(columns={"updates_only_visible_for_backers":"hidden_updates_complete_duration"})

# only verification
verify = project_level_data[["Project_Nr", "updates", "total_updates_complete_duration"]]
verify["equal"] = project_level_data["updates"] == project_level_data["total_updates_complete_duration"]

project_level_data = project_level_data.drop(["updates", "hidden_updates_complete_duration"], axis=1)

project_level_data["perc_hidden_updates"] = project_level_data["hidden_updates_within_duration"] / project_level_data["total_updates_within_duration"]


#%% continue with update level data
