# 1. download the important data from mysql db
# - category-wise (e.g. start with art)

# 2. filter the data to only what kind of projects we want to have in there
# - state: successful or failed (but not live or cancelled)

# 3. only keep the comments up until the deadline
# or as an alternative, keep all comments, but set a flag which comments were made before deadline
# in a really extra step, the comments after deadline could be used to assess performance after deadline
# because what we can see: with overfunded projects and high expectations, often delays,
# people complaining about the product not being shipped, ...

# 4. match the comments from our local xml files to the

import pandas as pd
import time
import os, os.path
from sqlalchemy import create_engine
import pytz
import datetime as dt

parent_directory = "/Users/philippsach/HiDrive/public/Kickstarter_Data"

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
    lambda x: "SELECT Project_Nr, Deadline, end_year, end_month, end_day, end_time, comments, withdrawn_comments\
    FROM combined_metadata WHERE (state = 'erfolgreich' OR state = 'Failed') AND category='" +
    x["category"] + "' LIMIT 10", axis=1)

test_query = "SELECT Project_Nr, Deadline, end_year, end_month, end_day, end_time, comments, withdrawn_comments,\
 state FROM combined_metadata WHERE (Project_Nr = '1000245024' OR Project_Nr = '10006131')"

# test_query uses one successful and one unsuccessful project
# have different patterns of how the date is stored

# category = category_df.iloc[0, 0]
# query = category_df.iloc[0, 2]
# print(category_df.iloc[0, 2])

def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def try_parsing_date(deadline, end_year, end_month, end_day, end_time):
    if isfloat(deadline):
        print("this is a float: ", deadline)
        # parsed_datetime = time.ctime(int(text))
        raw_string = end_year + "-" + end_month + "-" + end_day + " " + end_time
        print(raw_string)
        raw_parsed_datetime = dt.datetime.strptime(raw_string, "%Y-%m-%d %H:%M:%S")
        parsed_datetime = pytz.utc.localize(raw_parsed_datetime)
    else:
        print("this is not a float: ", deadline)
        raw_parsed_datetime = dt.datetime.strptime(deadline, "%Y-%m-%dT%H:%M:%S%z")
        parsed_datetime = pytz.utc.normalize(raw_parsed_datetime)
    return parsed_datetime


metadata = pd.read_sql(test_query, con=sqlEngine)
# print(metadata)


for row in metadata.itertuples(index=True, name="Project"):
    parsed_utc = try_parsing_date(
        deadline=row.Deadline,
        end_year=row.end_year,
        end_month=row.end_month,
        end_day=row.end_day,
        end_time=row.end_time)
    metadata.at[row.Index, "utcTime"] = parsed_utc


print(metadata)
