from selenium import webdriver
from bs4 import BeautifulSoup, NavigableString, Tag
import bs4
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import pandas as pd
import requests
from time import sleep
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
from datetime import datetime
import os
import urllib3
from urllib.request import urlopen as uReq

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def crawl_updates(soup):


    # options = webdriver.ChromeOptions()
    # options.add_argument("headless")
    # driver = webdriver.Chrome(executable_path="/Users/philippsach/.conda/envs/Python_Analysis/bin/chromedriver",
    #                           options=options)
    # driver.set_window_position(2000, 0)
    # driver.delete_all_cookies()
    # driver.get(new_link)
    # driver.switch_to.default_content()

    update = soup.find("a", {"data-content": "updates",
                             "id": "updates-emoji"})

    print("soup.title: ", soup.title)
    print("soup.title.text: ", soup.title.text)


    print("update: ", update)

    if update:
        update_count = update.find("span", {"class": "count"}).text
        print("update count is: ", update_count)

    return update_count


def get_all_input(link):
    try:
        print("=============================================================")
        print("Time: " + str(datetime.now()))
        print("Link: " + link)
        #sleep(5)

        new_l = link.split("?")[0]
        new_link = new_l + "/posts"

        
        options = webdriver.ChromeOptions()
        #options.add_argument("headless")
        
        driver = webdriver.Chrome(executable_path="/Users/philippsach/.conda/envs/Python_Analysis/bin/chromedriver",
                              options=options)
        
        driver.delete_all_cookies()
        driver.get(new_link)
        driver.switch_to.default_content()
        
        while True:
            try:
                showmore=WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH ,'//*[@id="project-post-interface"]/div/div[11]/div/div/div/button')))
                showmore.click()
            except TimeoutException:
                break
            except StaleElementReferenceException:
                break
        
        # //*[@id="project-post-interface"]/div/div[11]/div/div/div/button
        
        # //*[@id="project-post-interface"]/div/div[1]/div/div/a/div/article/footer/div/button/div
        # #project-post-interface > div > div:nth-child(1) > div > div > a
        # #project-post-interface > div > div:nth-child(2) > div > div > a
        # #project-post-interface > div > div:nth-child(16) > div > div > a
        # #project-post-interface > div > div:nth-child(1) > div > div > a
        
        #project-post-interface > div > div:nth-child(1) > div > div > a > div > article > footer > div > div > span:nth-child(1)
        #project-post-interface > div > div:nth-child(1) > div > div > a > div > article > footer > div > div > span:nth-child(2)
            
        # //*[@id="project-post-interface"]/div/div[1]/div/div/a
        # //*[@id="project-post-interface"]/div/div[1]/div/div/a/div/article/footer/div/div/span[1]
        
        # #project-post-interface > div > div:nth-child(1) > div > div > a
        
        # #project-post-interface > div > div:nth-child(1) > div > div > a > div > article > footer > div > button
        # //*[@id="project-post-interface"]/div/div[1]/div/div/a/div/article/footer/div/button
        soup = BeautifulSoup(driver.page_source, "lxml")
        
        children = driver.find_elements_by_css_selector('#project-post-interface > div > div > div >div > a[href]')
        children = driver.find_elements_by_css_selector('#project-post-interface > div > div')
        
        for child in children:
            
            comment_count = child.find_element_by_xpath('.//div/article/footer/div/div/span[1]').text
            like_count = child.find_element_by_xpath('.//div/article/footer/div/div/span[2]').text
            
            detail_link = child.get_attribute("href")
            
            # open up the specific sub-update in a new tab            
            driver.execute_script("window.open('{}');".format(detail_link))
            './/div/article/footer/div/button'
            
        ### this was the try with response and requests
        
        #response = requests.get(new_link)
        
        
        #print("response status code is: ", response.status_code)

        # if response.status_code == 200:
        #     print(response.status_code)
        #     soup = BeautifulSoup(response.text, "lxml")
        # else:
        #     # print("CONNECTION ERROR: " + str(response.status_code))
        #     if response.status_code == 429:
        #         print("TIMEOUT, OVERLOAD")
        #         sleep(60)
        #         print("Retry last link")
        #         response = requests.get(new_link, verify=False)
        #         # print(response.status_code)
        #         if response.status_code != 200:
        #             print("SOMETHING WENT WRONG --> Wait another interval")
        #             sleep(60)
        #             response = requests.get(new_link, verify=False)
        #             print(requests.status_code)
        #         soup = BeautifulSoup(response.text, "lxml")
        #     else:
        #         print("UNKNOWN ERROR - DO SOMETHING")
        #         print("<--------------------->")
        #         return
        
        

    except Exception as e:
        print("Error -> Mark Link as Error in Database")
        print(e)
        return

    updates_count = crawl_updates(soup)

    return


get_all_input(link="https://www.kickstarter.com/projects/1039804339/ben-newmans-matador")