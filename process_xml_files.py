import elementpath
import re
import os
import pandas as pd
from xml.etree import ElementTree as ET

directory = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/test/art/comments"
for entry in os.scandir(directory):
    if entry.path.endswith(".xml") and entry.is_file():
        print(entry.name) # THERE IS entry.name and entry.path :)

#tree = ET.parse("/Users/philippsach/Downloads/comment_107772823.xml")

# LATER: this information needs to come from the loop within the file directory.
# for now, define it as if it came from there the information
entry_name = "comment_pixeloccult.xml"

project_id = re.search(pattern = "comment_(.*?).xml", string = entry_name).group(1)

tree = ET.parse("/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/test/art/comments/comment_pixeloccult.xml")
root = tree.getroot()
print(root)
print(root.tag)
print(root[0].tag) # return first child of root element
print(root[0].attrib) # has no direct extra attributes in the food tag

print(len(root))

# FOR NOW: decided to not do it with parentCommentID, but with answerID
# combination of commentID and answerID with projectID will point on specific comment
# so that if answerID is None, then it is the comment itself - otherwise it is a corresponding answer
# WARNING: ONLY WORKS IF ANSWERS CANNOT BE NESTED ADDITIONALLY

df_cols = ["commentID", "answerID", "projectID", "commentOnlineID", "name", "title", "time", "content"]
rows = []

print("Now this is interesting")

for idx_comment, comments in enumerate(root):
    print("index is: " + str(idx_comment))
    print(comments)
    print(comments.tag)
    print("This is length of comments: " + str(len(comments)))

    c_online_id = comments[0].text
    c_name = comments[1].text
    c_title = comments[2].text
    c_time = comments[3].text
    c_content = comments[4].text

    rows.append(
        {"commentID": idx_comment + 1, # python is 0-based
         "answerID": None,
         "projectID": project_id,
         "commentOnlineID": c_online_id,
         "name": c_name,
         "title": c_title,
         "time": c_time,
         "content": c_content
         }
    )

    if len(comments) > 5:
        print("There are also answers")
        for idx_answer, answers in enumerate(comments[5:]):
            print(answers)
            a_comment_id = idx_comment +1
            a_answer_id = idx_answer + 1
            a_online_id = answers[0].text
            a_name = answers[1].text
            a_title = answers[2].text
            a_time = answers[3].text
            a_content = answers[4].text
            
            rows.append(
                {"commentID": a_comment_id,
                 "answerID": a_answer_id,
                 "projectID": project_id,
                 "commentOnlineID": a_online_id,
                 "name": a_name,
                 "title": a_title,
                 "time": a_time,
                 "content": a_content
                 }
            )
            
out_df = pd.DataFrame(rows, columns = df_cols)
print(out_df)
