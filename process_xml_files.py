import re
import os
import locale
import pytz
import datetime as dt
import time
import pandas as pd
from xml.etree import ElementTree as ET

# tree = ET.parse("/Users/philippsach/Downloads/comment_107772823.xml")


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


if __name__ == "__main__":

    # directory = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/test/art/comments"
    directory = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML_local_download/art/Comments"

    tic = time.time()
    comments_df = wrapper_process_xml(local_directory=directory)

    toc = time.time()
    print(toc - toc, "sec Elapsed")

    # tested for a specific case where content of a comment is empty
    # test_comments_df = pd.DataFrame([])
    # for entry in os.scandir(directory):
    #    if entry.path.endswith("mment_13619749.xml") and entry.is_file() and entry.stat().st_size>37:
    #        print("file: ", entry, " with size: ", entry.stat().st_size)
    #        test_comments_df = test_comments_df.append(transform_xml_to_dataframe(entry))

    # possibility to save the dataframe as a csv to also manually have a look at it
    # comments_df.to_csv("/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/test/full_comments_df_art_test.csv")
