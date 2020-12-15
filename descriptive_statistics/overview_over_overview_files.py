#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 10:59:49 2020

@author: philippsach
"""

import pandas as pd
import os

overview_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Python_Analysis/data/overview_files/"
save_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/test/"

files = os.listdir(overview_path)
exclude_string = "current"
relevant_files = [str for str in files if exclude_string not in str]

complete_overview_file = pd.DataFrame()

for file in relevant_files: 
    print("current file: ", file)
    df = pd.read_csv(os.path.join(overview_path, file))
    df["category_string"] = str(file)
    complete_overview_file = complete_overview_file.append(df)
    
    
complete_overview_file = complete_overview_file.sort_values(by="category_string")

complete_overview_file.to_csv(save_path + "complete_overview_file.csv")
