#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 17:56:41 2020

@author: philippsach
"""

import locale
import datetime as dt
import time
import pytz
import os


datestring1 = "1435895563"
datestring2 = "2019-04-24T12:04:26-04:00"
datestring3 = "2019-04-24T12:04:26"

target_tz = pytz.timezone("UTC")
os.environ['TZ'] = 'Europe/Berlin'

print(dt.datetime.strptime(datestring3, "%Y-%m-%dT%H:%M:%S"))

print(dt.datetime.strptime(datestring2, "%Y-%m-%dT%H:%M:%S%z"))


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def try_parsing_date(deadline, end_year, end_month, end_day, end_time):
    if isfloat(deadline):
        print("this is a float: ", deadline)
        #parsed_datetime = time.ctime(int(text))
        raw_string = end_year + "-" + end_month + "-" + end_day + " " + end_time
        print(raw_string)
        raw_parsed_datetime = dt.datetime.strptime(raw_string, "%Y-%m-%d %H:%M:%S")
        parsed_datetime = pytz.utc.localize(raw_parsed_datetime)
    else:
        print("this is not a float: ", deadline)
        raw_parsed_datetime = dt.datetime.strptime(deadline, "%Y-%m-%dT%H:%M:%S%z")
        parsed_datetime = pytz.utc.normalize(raw_parsed_datetime)
    return parsed_datetime



for row in metadata.itertuples(index=True, name="Project"):
    
    parsed_utc = try_parsing_date(
        deadline = row.Deadline,
        end_year = row.end_year,
        end_month = row.end_month,
        end_day = row.end_day,
        end_time = row.end_time)
    metadata.at[row.Index, "utcTime"] = parsed_utc

