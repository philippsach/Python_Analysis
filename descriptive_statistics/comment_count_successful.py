#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 09:37:25 2020

@author: philippsach
"""


import pandas as pd
from sqlalchemy import create_engine 
import seaborn as sns
from scipy.stats import mstats

query = "SELECT Project_Nr, comments, state, category FROM combined_metadata WHERE (state = 'Successful' OR state = 'FAILED') "

sqlEngine = create_engine("mysql+pymysql://phil_sach:entthesis2020@85.214.204.221/thesis")

comments_all_projects = pd.read_sql(query, con=sqlEngine)

basic_statistics = comments_all_projects["comments"].describe()

category_statistics = comments_all_projects.groupby("category")["comments"].describe()
success_statistics = comments_all_projects.groupby("state")["comments"].describe()

category_and_success_statistics = comments_all_projects.groupby(["state", "category"]).describe()

comments_successful = comments_all_projects[comments_all_projects["state"] == "Successful"].copy()
comments_unsuccessful = comments_all_projects[comments_all_projects["state"] == "FAILED"].copy()

# create plot of distribution
density_plot = sns.distplot(comments_successful["comments"], hist = False)

boxplot = sns.boxplot(x=comments_successful["comments"])
boxplot_unsuccessful = sns.boxplot(x=comments_unsuccessful["comments"])


# clipping values with winsorizing (not eliminating outliers, but giving them values of percentile)
comments_successful["comments"] = pd.Series(mstats.winsorize(comments_successful["comments"], limits = [0.01, 0.01]))
comments_unsuccessful["comments"] = pd.Series(mstats.winsorize(comments_unsuccessful["comments"], limits = [0.01, 0.01]))

boxplot_winsorized = sns.boxplot(x=comments_successful["comments"])
boxplot_winsorized_unsuccessful = sns.boxplot(x=comments_unsuccessful["comments"])
density_winsorized = sns.distplot(comments_successful["comments"], hist=False)


# also in papaer incorporating comment text into success:
# removed those projects which had no comments at all in their analysis

comments_unsuccessful_min_1_comments = comments_unsuccessful[comments_unsuccessful["comments"] >0]
