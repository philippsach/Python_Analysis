#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 09:58:30 2021

@author: philippsach
"""



link = overview_file["Link"].iloc[243]


for attempt in range(5):
    if driver.find_element_by_class_name("page-title").text == "Please verify you are a human":
        print("mierda")
    







    
    
    /html/body/section/div[2]/div
    /html/body/section/div[2]/div

if driver.find_element_by_xpath("//div[text()='Please verify you are a human']"):
    print("yes")
    
    /html/body/a

if driver.find_element_by_xpath("html/body/a"):
    test = driver.find_element_by_xpath("html/body/a").get_attribute("href")
    driver.get(test)
    
    personal_info.find_element_by_class_name("comment-link").get_attribute("href")
    
    
if driver.find_element_by_class_name("page-title"):
        print("yes")


    if driver.find_elements_by_xpath("//*[contains(text(), 'Please verify you are a human')]"):
        print("yes")    