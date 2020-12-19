#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 19 16:51:09 2020

@author: philippsach
"""


import re
import os
import locale
import pytz
import datetime as dt
import time
import pandas as pd
from xml.etree import ElementTree as ET

update_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML_local_download/art/Updates/update_1039804339.xml"

tree = ET.parse(update_path)
root = tree.getroot()

df_cols = ["ID", "name", "author_title", "time", "like_count", "comment_count",
           "update_title", "picture_count", "gif_count", "video_count",
           "content"]

rows = []

for node in root:
    u_id = node.find("ID").text
    u_name = node.find("Name").text
    u_author_title = node.find("AuthorTitle").text
    u_time = node.find("Time").text
    u_like_count = node.find("LikesCount").text
    u_comment_count = node.find("CommentCount").text
    u_update_title = node.find("UpdateTitle").text
    u_picture_count = node.find("PictureCount").text
    u_gif_count = node.find("GifCount").text
    u_video_count = node.find("VideoCount").text
    u_content = node.find("Content").text
    
    rows.append({"ID": u_id, "name": u_name, "author_title": u_author_title,
                 "time": u_time, "like_count": u_like_count, "comment_count": u_comment_count,
                 "update_title": u_update_title, "picture_count": u_picture_count, "gif_count": u_gif_count,
                 "video_count": u_video_count, "content": u_content})
    

out_df = pd.DataFrame(rows, columns = df_cols)