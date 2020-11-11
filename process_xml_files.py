import elementpath
import re
import pandas as pd
from xml.etree import ElementTree as ET

tree = ET.parse("/Users/philippsach/Downloads/comment_107772823.xml")
root = tree.getroot()
print(root)
print(root.tag)
print(root[0].tag) # return first child of root element
print(root[0].attrib) # has no direct extra attributes in the food tag

print(len(root))

df_cols = ["ID", "Name", "Title", "Time", "Content"]
rows = []

print("Now this is interesting")

for comments in root:
    print(comments)
    print("This is length of comments: " + str(len(comments)))
    c_id = comments[0].text
    c_name = comments[1].text
    c_title = comments[2].text
    c_time = comments[3].text
    c_content = comments[4].text

    rows.append(
        {"ID": c_id,
         "Name": c_name,
         "Title": c_title,
         "Time": c_time,
         "Content": c_content})

out_df = pd.DataFrame(rows, columns = df_cols)
print(out_df)
