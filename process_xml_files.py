#%% import packages

import re
import os
import locale
import pytz
import datetime as dt
import time
import pandas as pd
from xml.etree import ElementTree as ET

from selenium import webdriver
import urllib3
from time import sleep

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# tree = ET.parse("/Users/philippsach/Downloads/comment_107772823.xml")

#%% user settings
overview_file_needs_deadline_update = False
change_the_deadlines = True

#%% define functions

# LATER: this information needs to come from the loop within the file directory.
# for now, define it as if it came from there the information
def remove_new_lines(string_with_line_break):
    # print(string_with_line_break)
    string_without_line_break = string_with_line_break.replace("\n", " ")
    return string_without_line_break


def try_parsing_date(text):
    # formats: "%d. %B %Y %H:%M %Z" for old format; "%B %d, %Y %I:%M %p %Z"
    # for new format on Mac due to english system settings
    for fmt in ("%d. %B %Y %H:%M %Z", "%B %d, %Y %I:%M %p %Z"):
        try:
            if fmt == "%d. %B %Y %H:%M %Z":
                locale.setlocale(locale.LC_ALL, "de_DE")
                # print("try German format of string: ", text)
            elif fmt == "%B %d, %Y %I:%M %p %Z":
                locale.setlocale(locale.LC_ALL, "en_US")
                # print("try english format of string: ", text)

            return dt.datetime.strptime(text, fmt)

        except ValueError:
            pass
        # raise ValueError("no valid date format found")


def calc_utc_time_from_string(str_datetime):
    # locale.setlocale(locale.LC_ALL, "de_DE")
    # naive_datetime = dt.datetime.strptime(str_datetime, "%d. %B %Y %H:%M %Z")
    naive_datetime = try_parsing_date(str_datetime)
    str_timezone_info = str_datetime[(str_datetime.rfind(" ") + 1):]

    if str_timezone_info == "CEST":  # is already aware in "CET" of difference of summer and winter time
        str_timezone_info = "CET"

    target_tz = pytz.timezone("UTC")
    local_tz = pytz.timezone(str_timezone_info)

    localized_datetime = local_tz.localize(naive_datetime)
    utc_datetime = target_tz.normalize(localized_datetime)

    return utc_datetime


def transform_updates_xml_to_dataframe(entry):
    # print("update entry path that is being transformed: ", entry.path)
    tree = ET.parse(entry.path)
    root = tree.getroot()
    project_id = re.search(pattern="update_(.*?).xml", string=entry.name).group(1)

    df_cols = ["projectID", "updateID", "name", "author_title", "time", "only_visible_for_backers", "like_count",
               "comment_count", "update_title", "picture_count", "gif_count",
               "video_count", "content"]

    rows = []

    for node in root:
        u_id = node.find("ID").text
        u_name = node.find("Name").text
        u_author_title = node.find("AuthorTitle").text
        u_time = node.find("Time").text
        u_only_backers_visible = node.find("OnlyVisibleForBackers").text
        u_like_count = node.find("LikesCount").text
        u_comment_count = node.find("CommentCount").text
        u_update_title = node.find("UpdateTitle").text
        u_picture_count = node.find("PictureCount").text
        u_gif_count = node.find("GifCount").text
        u_video_count = node.find("VideoCount").text
        u_content = node.find("Content").text

        rows.append({"projectID": project_id, "updateID": u_id, "name": u_name, "author_title": u_author_title,
                     "time": u_time, "only_visible_for_backers": u_only_backers_visible, "like_count": u_like_count, 
                     "comment_count": u_comment_count, "update_title": u_update_title, "picture_count": u_picture_count, 
                     "gif_count": u_gif_count, "video_count": u_video_count, "content": u_content})

    out_df = pd.DataFrame(rows, columns=df_cols)

    # HERE: INSERT CHANGING OF DATATYPES AND PARSING FOR EXAMPLE THE DATE OF THE UPDATE
    # from string: "July 18, 2017" to date object: 2018-07-18 or so
    #

    return out_df


def transform_xml_to_dataframe(entry):
    tree = ET.parse(entry.path)
    root = tree.getroot()
    project_id = re.search(pattern="comment_(.*?).xml", string=entry.name).group(1)

    # general definitions, could maybe be done outside of the function
    df_cols = ["commentID", "answerID", "projectID", "commentOnlineID",
               "name", "title", "strTime", "utcTime", "content"]
    rows = []

    for idx_comment, comments in enumerate(root):

        c_online_id = comments[0].text[8:]
        c_name = comments[1].text
        c_title = comments[2].text
        c_str_time = comments[3].text

        if comments[4].text is not None:
            c_content = remove_new_lines(string_with_line_break=comments[4].text)
        else:
            c_content = ""
        c_utc_time = calc_utc_time_from_string(c_str_time)

        rows.append(
            {"commentID": idx_comment + 1,  # python is 0-based
             "answerID": None,
             "projectID": project_id,
             "commentOnlineID": c_online_id,
             "name": c_name,
             "title": c_title,
             "strTime": c_str_time,
             "utcTime": c_utc_time,
             "content": c_content
             }
        )

        if len(comments) > 5:
            # print("There are also answers")
            for idx_answer, answers in enumerate(comments[5:]):
                # print(answers)
                a_comment_id = idx_comment + 1
                a_answer_id = idx_answer + 1
                a_online_id = answers[0].text[(answers[0].text.find("reply") + 6):]
                a_name = answers[1].text
                a_title = answers[2].text
                a_str_time = answers[3].text
                a_utc_time = calc_utc_time_from_string(a_str_time)
                a_content = remove_new_lines(string_with_line_break=answers[4].text)

                rows.append(
                    {"commentID": a_comment_id,
                     "answerID": a_answer_id,
                     "projectID": project_id,
                     "commentOnlineID": a_online_id,
                     "name": a_name,
                     "title": a_title,
                     "strTime": a_str_time,
                     "utcTime": a_utc_time,
                     "content": a_content
                     }
                )

    out_df = pd.DataFrame(rows, columns=df_cols)
    return out_df


def wrapper_process_xml(local_directory):
    local_comments_df = pd.DataFrame([])

    for entry in os.scandir(local_directory):
        if entry.path.endswith(".xml") and entry.is_file() and entry.stat().st_size > 37:
            print("file: ", entry, " with size: ", entry.stat().st_size)
            local_comments_df = local_comments_df.append(transform_xml_to_dataframe(entry))

    return local_comments_df


def wrapper_process_updates_xml(local_directory):
    local_updates_df = pd.DataFrame([])

    for entry in os.scandir(local_directory):
        if entry.path.endswith(".xml") and entry.is_file():
            print("file: ", entry, " with size: ", entry.stat().st_size)
            local_updates_df = local_updates_df.append(transform_updates_xml_to_dataframe(entry))

    return local_updates_df


def download_start_and_end_date(overview_file):
    
    
    
    return 1

#%% function calls

if __name__ == "__main__":

    directory = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML_local_download/art/Comments"
    unchanged_metadata_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Python_Analysis/data/overview_files"
    adapted_metadata_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/00_FINAL"
    save_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Austausch Daniel Philipp/datasets for coding"
    statistics_save_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/00_FINAL"

    update_directory_tech = "/Users/philippsach/HiDrive/public/Kickstarter_Data/technology/Updates"
    updates_df_tech = wrapper_process_updates_xml(local_directory=update_directory_tech)
    updates_df_tech["category"] = "technology"
    
    update_directory_design = "/Users/philippsach/HiDrive/public/Kickstarter_Data/design/Updates"
    updates_df_design = wrapper_process_updates_xml(local_directory=update_directory_design)
    updates_df_design["category"] = "design"
    
    updates_df = updates_df_tech.append(updates_df_design)
    
    

    
   
    
    if overview_file_needs_deadline_update:
        print("downloading the deadline dates from Kicktraq.")
    
        # OPTION 1) COMPLETELY NEW CREATION
        if False:
            overview_file = pd.read_csv(os.path.join(unchanged_metadata_path, "update_metadata.csv"))
            overview_file = overview_file.rename({"Project_Nr": "projectID"}, axis=1)
            
            overview_file = overview_file.rename({"deadline_date": "deadline_date_sql"}, axis=1)
            
            overview_file["deadline_date_sql"] = pd.to_datetime(overview_file["deadline_date_sql"], format = "%Y-%m-%d")
            overview_file["deadline_date_sql"] = overview_file["deadline_date_sql"].dt.date
            
            overview_file["start_date_sql"] = overview_file.apply(lambda x: x["deadline_date_sql"] - dt.timedelta(x["Duration"]), axis=1)
            
            
            
            # ADD KICKTRAQ LINK TO DATA
            overview_file["Link"] = overview_file["Link"].apply(lambda x: x.split("?")[0])
            overview_file["Link"] = overview_file["Link"].str.rstrip()
            
            overview_file.rename(columns={"Link": "original_link"}, inplace=True)
            overview_file.insert(1, "kicktraq_link", overview_file["original_link"])
            
            overview_file["kicktraq_link"] = overview_file["kicktraq_link"].str.replace("kickstarter", "kicktraq")
            overview_file["kicktraq_link"] = overview_file["kicktraq_link"] + "/#chart-daily"
            
            # ADD EMPTY COLUMNS WHERE RESULT OF KICKTRAQ WILL BE SAVED
            overview_file["start_date_text"] = ""
            overview_file["start_date_title"] = ""
            overview_file["end_date_text"] = ""
            overview_file["end_date_title"] = ""
        
        # OPTION 2: START FROM FILE THAT ALREADY EXISTS
        overview_file = pd.read_csv(os.path.join(adapted_metadata_path, "deadlines_overview_file.csv"))
        overview_file["start_date_text"] = overview_file["start_date_text"].astype(str)
        overview_file["start_date_title"] = overview_file["start_date_title"].astype(str)
        overview_file["end_date_text"] = overview_file["end_date_text"].astype(str)
        overview_file["end_date_title"] = overview_file["end_date_title"].astype(str)
        
        overview_file["start_date_text"] = overview_file["start_date_text"].str.replace(pat="nan", repl="")
        overview_file["start_date_title"] = overview_file["start_date_title"].str.replace(pat="nan", repl="")
        overview_file["end_date_text"] = overview_file["end_date_text"].str.replace(pat="nan", repl="")
        overview_file["end_date_title"] = overview_file["end_date_title"].str.replace(pat="nan", repl="")
        
        
        # GET THE START AND END DATE FROM KICKTRAQ
        options = webdriver.ChromeOptions()
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        options.add_argument("--disable-blink-features")
        options.add_argument("--disable-blink-features=AutomationControlled")
        
        options.add_argument("window-size=1280,800")
        options.add_argument("Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36")
        
        #options.add_argument("headless")
        
        driver = webdriver.Chrome(executable_path="/Users/philippsach/.conda/envs/Python_Analysis/bin/chromedriver_copy",
                              options=options)
        
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        driver.delete_all_cookies()
        
        for row in overview_file.itertuples(index=True, name="Project"):
            
            if row.start_date_text == "":
            
                link = row.kicktraq_link
                
                print("LINK is: ", link)
                
                sleep(0.5)
                driver.get(link)
                sleep(0.5)
                
                start_date_element = driver.find_element_by_xpath('//*[@id="project-info-text"]/a[1]')
                start_date_text = start_date_element.text
                start_date_title = start_date_element.get_attribute("title")
                
                end_date_element = driver.find_element_by_xpath('//*[@id="project-info-text"]/a[2]')
                end_date_text = end_date_element.text
                end_date_title = end_date_element.get_attribute("title")
                
                overview_file.at[row.Index, "start_date_text"] = start_date_text
                overview_file.at[row.Index, "start_date_title"] = start_date_title
                overview_file.at[row.Index, "end_date_text"] = end_date_text
                overview_file.at[row.Index, "end_date_title"] = end_date_title
            
            
        
        driver.close()
        
        overview_file.to_csv(os.path.join(adapted_metadata_path, "deadlines_overview_file.csv"), index=False)
        
    else:
        overview_file = pd.read_csv(os.path.join(adapted_metadata_path, "deadlines_overview_file.csv"))
        overview_file["deadline_date_sql"] = pd.to_datetime(overview_file["deadline_date_sql"])
        overview_file["deadline_date_sql"] = overview_file["deadline_date_sql"].dt.date
    
    overview_file["group_category"] = ""
    overview_file["group_category"].iloc[0:250] = "technology_part1"
    overview_file["group_category"].iloc[250:500] = "technology_part2"
    
    overview_file["group_category"].iloc[500:750] = "design_part2"
    overview_file["group_category"].iloc[750:1000] = "design_part1"
    
    
    if change_the_deadlines:
        # CHANGE THE DEADLINE STUFF
        overview_file["end_date_year"] = overview_file["deadline_date_sql"].apply(lambda x: x.year)
        overview_file["end_date_string"] = overview_file["end_date_text"] + ", " + overview_file["end_date_year"].astype("str")
        overview_file["end_date_string"] = overview_file["end_date_string"].str.replace(pat="st|nd|rd|th", repl="")
        
        overview_file["deadline_date_kicktraq"] = pd.to_datetime(overview_file["end_date_string"], format="%b %d, %Y")
        overview_file["deadline_date_kicktraq"] = overview_file["deadline_date_kicktraq"].dt.date
        
        overview_file["deadlines_equal"] = overview_file["deadline_date_kicktraq"] == overview_file["deadline_date_sql"]
        overview_file["deadlines_difference"] = overview_file.apply(lambda x: (x["deadline_date_sql"] - x["deadline_date_kicktraq"]).days if not pd.isnull(x["deadline_date_kicktraq"]) else 1000, axis=1)
        
        # WARNING: THIS IS FILTER !!!! 
        # overview_file = overview_file[overview_file["category"] == "technology"]
        
        overview_file["deadline_date"] = overview_file.apply(lambda x: x["deadline_date_kicktraq"] if abs(x["deadlines_difference"]) <= 1 else x["deadline_date_sql"], axis=1)
        overview_file["Duration"] = overview_file.apply(lambda x: x["Duration"] - 1 if x["deadlines_difference"] == 1 else x["Duration"], axis=1)
    
        overview_file["start_date"] = overview_file.apply(lambda x: x["deadline_date"] - dt.timedelta(days= x["Duration"]), axis=1)
    
    
    join_overview_file = overview_file[[
        "category",
        "group_category",
        "projectID",
        "start_date",
        "deadline_date",
        "deadline_date_sql",
        "end_date_text",
        "state",
        "Duration",
        "goal_amount",
        "pledged_amount",
        "original_link",
        "deadlines_difference"
        ]]
    
    
    
    final_updates_df = updates_df.merge(join_overview_file, how="left", on=["category", "projectID"])
    
    # NEEDED TO DO DEBUGGING HERE !!! - maybe need to delete later
    final_updates_df["time"] = pd.to_datetime(final_updates_df["time"], format = "%B %d, %Y")
    final_updates_df["time"] = final_updates_df["time"].dt.date
    # END OF DEBUGGING
    
    
    # how many updates in total obtained
    update_statistics_df = updates_df.groupby("projectID").count()["updateID"].reset_index().rename(columns={"updateID": "total_updates_complete_duration"})

    
    coding_updates_df = final_updates_df[final_updates_df["time"] <= final_updates_df["deadline_date"]]
    
    updates_within_project_duration = coding_updates_df.groupby("projectID").count()["updateID"].reset_index().rename(columns={"updateID": "total_updates_within_duration"})
    
    updates_within_project_duration_hidden = coding_updates_df.groupby(["projectID", "only_visible_for_backers"]).count()["updateID"].reset_index()
    updates_within_project_duration_hidden = updates_within_project_duration_hidden[updates_within_project_duration_hidden["only_visible_for_backers"] == "True"]
    updates_within_project_duration_hidden = updates_within_project_duration_hidden.pivot(
        values="updateID", 
        columns="only_visible_for_backers", 
        index="projectID"
        ).reset_index()  \
    .fillna(0)
    updates_within_project_duration_hidden = updates_within_project_duration_hidden.rename(columns={"True":"hidden_updates_within_duration"})
    
    update_statistics_df = update_statistics_df.merge(updates_within_project_duration_hidden, how="left", on="projectID")
    update_statistics_df = update_statistics_df.merge(updates_within_project_duration, how="left", on="projectID")
    update_statistics_df = update_statistics_df.fillna(0)
    
    coding_updates_df = coding_updates_df[coding_updates_df["only_visible_for_backers"] == "False"]
     
    
    # manually need to adjust one project
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/1853778193/boxbotix-an-open-source-modular-robotics-platform", "start_date"] = dt.date(2015,12,1)
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/1853778193/boxbotix-an-open-source-modular-robotics-platform", "Duration"] = 20
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/1853778193/boxbotix-an-open-source-modular-robotics-platform", "day_within_campaign"] = 0
    
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/outdoorvitals/lofttek-adventure-jacket", "start_date"] = dt.date(2018, 10, 30)
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/outdoorvitals/lofttek-adventure-jacket", "Duration"] = 31
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/outdoorvitals/lofttek-adventure-jacket", "day_within_campaign"] = 0
    
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/1193288998/varia-6-piece-furniture-collection-creates-25-uses", "start_date"] = dt.date(2018, 8, 1)
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/1193288998/varia-6-piece-furniture-collection-creates-25-uses", "Duration"] = 36
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/1193288998/varia-6-piece-furniture-collection-creates-25-uses", "day_within_campaign"] = 0
    
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/georgedawson/houston-climbing-co-op-inner-loop-join-us-build-th", "start_date"] = dt.date(2015, 4, 27)
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/georgedawson/houston-climbing-co-op-inner-loop-join-us-build-th", "Duration"] = 35
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/georgedawson/houston-climbing-co-op-inner-loop-join-us-build-th", "day_within_campaign"] = 0
    
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/1515202019/little-grey-aliens", "start_date"] = dt.date(2012, 2, 20)
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/1515202019/little-grey-aliens", "Duration"] = 29
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/1515202019/little-grey-aliens", "day_within_campaign"] = 0
    
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/morsel/morsel-spork", "start_date"] = dt.date(2018, 3, 19)
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/morsel/morsel-spork", "Duration"] = 33
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/morsel/morsel-spork", "day_within_campaign"] = 0
    
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/sleeklifedesign/vext-slim-the-sleek-functional-minimalist-wallet", "start_date"] = dt.date(2020, 3, 12)
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/sleeklifedesign/vext-slim-the-sleek-functional-minimalist-wallet", "Duration"] = 37
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/sleeklifedesign/vext-slim-the-sleek-functional-minimalist-wallet", "day_within_campaign"] = 0
    
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/h2joe/h2joe", "start_date"] = dt.date(2019, 2, 12)
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/h2joe/h2joe", "Duration"] = 31
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/h2joe/h2joe", "day_within_campaign"] = 0
    
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/xopherdeep/umeos-the-21st-century-you-me-os-welcome-to-the-fu", "start_date"] = dt.date(2014, 11, 20)
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/xopherdeep/umeos-the-21st-century-you-me-os-welcome-to-the-fu", "Duration"] = 58
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/xopherdeep/umeos-the-21st-century-you-me-os-welcome-to-the-fu", "day_within_campaign"] = 0
    
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/385821774/sos-watch-life-perserver-saves-lives", "start_date"] = dt.date(2012,2,6)
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/385821774/sos-watch-life-perserver-saves-lives", "Duration"] = 32
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/385821774/sos-watch-life-perserver-saves-lives", "day_within_campaign"] = 0

    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/9084249/flink-a-cloud-photo-frame-for-family-and-friends", "start_date"] = dt.date(2014, 2, 21)
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/9084249/flink-a-cloud-photo-frame-for-family-and-friends", "Duration"] = 30
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/9084249/flink-a-cloud-photo-frame-for-family-and-friends", "day_within_campaign"] = 0
    
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/1232469188/occam-cycle-the-simplest-folding-bike-for-everyday", "start_date"] = dt.date(2014, 8, 28)
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/1232469188/occam-cycle-the-simplest-folding-bike-for-everyday", "Duration"] = 37
    coding_updates_df.loc[coding_updates_df["original_link"] == "https://www.kickstarter.com/projects/1232469188/occam-cycle-the-simplest-folding-bike-for-everyday", "day_within_campaign"] = 0
    
    
    
    coding_updates_df["day_within_campaign"] = coding_updates_df.apply(lambda x: (x["time"] - x["start_date"]).days, axis=1)
    coding_updates_df["time_till_deadline"] = coding_updates_df.apply(lambda x: (x["deadline_date"] - x["time"]).days, axis=1)
    
    coding_updates_df["first_seven_days"] = coding_updates_df.apply(lambda x: x["day_within_campaign"] <= 6,axis=1)
    coding_updates_df["last_seven_days"] = coding_updates_df.apply(lambda x: x["time_till_deadline"] <= 6, axis=1)

    coding_updates_df["project_updateID"] = coding_updates_df["projectID"] + "_" + coding_updates_df["updateID"]  
    
    # coding_updates_first_tech_projects = coding_updates_df[coding_updates_df["category"] == "technology"]
    # coding_updates_first_design_projects = coding_updates_df[coding_updates_df["category"] == "design"]
    
    # coding_updates_first_tech_projects = coding_updates_first_tech_projects[[
    #     "projectID",
    #     "updateID",
    #     "time",
    #     "deadline_date",
    #     "update_title",
    #     "content"
    #     ]]
    
    # coding_updates_first_design_projects = coding_updates_first_design_projects[[
    #     "projectID",
    #     "updateID",
    #     "time",
    #     "deadline_date",
    #     "update_title",
    #     "content"
    #     ]]
    
    
    save_coding_updates_df = coding_updates_df[[
        "category",
        "group_category",
        "project_updateID",
        "original_link",
        "time",
        "deadline_date",
        "day_within_campaign",
        "first_seven_days",
        "last_seven_days",
        "state",
        "Duration",
        "goal_amount",
        "pledged_amount",
        "picture_count",
        "gif_count",
        "video_count",
        "like_count",
        "comment_count",
        "update_title",
        "content"
        ]]
    
    save_coding_updates_tech = save_coding_updates_df[save_coding_updates_df["category"] == "technology"]
    save_coding_updates_design = save_coding_updates_df[save_coding_updates_df["category"] == "design"]
    
    save_coding_updates_tech.drop("category", inplace=True, axis=1)
    save_coding_updates_design.drop("category", inplace=True, axis=1)
    
    excel_writer = pd.ExcelWriter(os.path.join(save_path, "coding_updates_tech.xlsx"), engine="xlsxwriter")
    save_coding_updates_tech.to_excel(excel_writer, sheet_name="data")
    excel_writer.save()
    
    excel_writer_design = pd.ExcelWriter(os.path.join(save_path, "coding_updates_design.xlsx"), engine="xlsxwriter")
    save_coding_updates_design.to_excel(excel_writer_design, sheet_name="data")
    excel_writer_design.save()
    
    update_statistics_df.to_csv(os.path.join(statistics_save_path, "updates_and_hidden_updates_within_duration.csv"), index=False)
    
    ### this is needed for parsing comments

    # tic = time.time()
    # comments_df = wrapper_process_xml(local_directory=directory)
    # toc = time.time()
    # print(toc - toc, "sec Elapsed")
