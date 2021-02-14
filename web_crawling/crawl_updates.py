#%% import packages

from selenium import webdriver
from bs4 import BeautifulSoup, NavigableString, Tag
import bs4
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.common.exceptions import ElementClickInterceptedException, ElementNotInteractableException
import pandas as pd
import numpy as np
import requests
from time import sleep
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
from datetime import datetime
import os
import urllib3
from urllib.request import urlopen as uReq
from random import random

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


#%% definition of function

def crawl_updates(link, path, pr_nr):
    try:
        print("=============================================================")
        print("Time: " + str(datetime.now()))
        print("Link: " + link)
        #sleep(5)
    
        
        new_l = link.split("?")[0]
        new_link = new_l + "/posts"

        
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
        driver.get(new_link)
        driver.switch_to.default_content()
        
        # try to catch the case when automatic scraping was detected
        if driver.find_elements_by_class_name("page-title"):
            if driver.find_element_by_class_name("page-title").text == "Please verify you are a human":
                print("mierda, we got blown")
                sleep(10)  # this time is needed for manual clicking of PRESS AND HOLD
                input("Press ENTER to continue ... ")
        
        project_hidden = False
        
        if driver.find_elements_by_id("hidden_project"):
            print("project is a hidden project")
            project_hidden = True
            only_visible_for_backers_count = np.nan
            faq = np.nan
        
        if driver.find_elements_by_xpath('//*[@id="project-post-interface"]/div/h3'):
            print("we are having troubles loading updates right now...")
            sleep(10)  
            input("Press ENTER to continue ... ")
        
        # only continue crawling if project is not hidden
        if not project_hidden:
            
            while True:
                try:
                    showmore=WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH ,'//*[@id="project-post-interface"]/div/div[last()]/div/div/div/button')))
                    showmore.click()
                except TimeoutException:
                    break
                except StaleElementReferenceException:
                    break
            
            
            #soup = BeautifulSoup(driver.page_source, "lxml")
            
            # extract the FAQ count
            faq = driver.find_element_by_css_selector("#faq-emoji").get_attribute("emoji-data")
            #print("faq is: ", faq)
            
            # archive: children would contain the updates that are visible for everybody
            #children = driver.find_elements_by_css_selector('#project-post-interface > div > div > div >div > a[href]')
            
            # all children elements
            children_all = driver.find_elements_by_css_selector('#project-post-interface > div > div > div >div')
    
            update_list = []
            
            only_visible_for_backers_count = 0
                    
            for idx, child in enumerate(children_all):
                
                sleep(1.5*random())
                
                # case that it is a visble update
                if child.find_elements_by_xpath(".//a/div"):
                    only_visible_for_backers = False
                    
                    update_child = child.find_element_by_xpath(".//a")
                
                    print("now crawling children: ", idx, " which is VISIBLE. of project: ", pr_nr)
                    
                    comment_count = update_child.find_element_by_xpath('.//div/article/footer/div/div/span[1]').text
                    like_count = update_child.find_element_by_xpath('.//div/article/footer/div/div/span[2]').text
                    
                    detail_link = update_child.get_attribute("href")
                    
                    # open up the specific sub-update in a new tab            
                    driver.execute_script("window.open('{}');".format(detail_link))
                    
                    driver.switch_to.window(driver.window_handles[1])
                    
                    
                    
                    
                    # need to wait until header is found, while switching and loading new page could not find
                    header = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, 
                             '#project-post-interface > div > div > div > article > header'
                             )
                            )
                        )
                    #header = driver.find_element_by_css_selector('#project-post-interface > div > div > div > article > header')
                    
                    # check if there are troubles to load this post right now
                    if header.find_elements_by_xpath('.//div/h3'):
                            
                        # then there are trouble loading this post
                        print("Having trouble loading this post right now.")
                        sleep(10)  # this time is needed for manual clicking of PRESS AND HOLD
                        input("Press ENTER to continue ... ")
                    
                    update_id = header.find_element_by_xpath('.//div[1]/div/span').text
                    update_title = header.find_element_by_class_name('mb3').text
                    
                    personal_info = header.find_element_by_xpath('.//div[2]/div[2]/div')
                    raw_name = personal_info.text
                    title_name = personal_info.find_element_by_xpath('.//span').text
                    name = raw_name.replace(title_name, '')
                                
                    time = header.find_element_by_xpath('div[2]/div[2]/span').text
                                
                    # iframe = driver.find_element_by_class_name("rte__content")
                    
                    iframe = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located(
                            (By.CLASS_NAME, 'rte__content')
                            )
                        )
                    
                    update_text = iframe.text
                    
                    pics = iframe.find_elements_by_tag_name("img")
                    vids = iframe.find_elements_by_tag_name("video")
                    video_count = len(vids)
                    
                    gif_counter = 0
                    pic_counter = 0
                    
                    # directly embedded gifs and videos affect also the picture count
                    for pic in pics:
                        if pic.get_attribute("data-src") is not None:
                            # print("here we are")
                            if ".gif" in pic.get_attribute("data-src"):
                                # print("gif")
                                gif_counter +=1
                            else:
                                # print("pic")
                                pic_counter +=1
                                
                        else:
                             # videos that are directly embedded in Kickstarter are also counted as pictures - reduce that
                            if pic.get_attribute("alt") is not None:
                                if pic.get_attribute("alt") != " project video thumbnail":
                                    pic_counter +=1
                            else: 
                                pic_counter +=1
                    
                    embed_elements = iframe.find_elements_by_class_name("embedly-card-hug")
                    
                    for embed_element in embed_elements:
                        # try to click and open the link in a new window 
                        
                        sleep(2)
                        
                        # try to click the button
                        try: 
                            old_length = len(driver.window_handles)
                            sleep(2)
                            embed_element.click()
                            sleep(2)
                            new_length = len(driver.window_handles)
                            
                            # check if new tab has been opened
                            if new_length > old_length: 
                                driver.switch_to.window(driver.window_handles[new_length-1])
                                
                                if "giphy" in driver.current_url:
                                    print("this is an external gif link")
                                    gif_counter += 1
                                
                                driver.close()
                                driver.switch_to.window(driver.window_handles[old_length - 1])
                                sleep(1)
                            
                            else:
                                "this seems to be a youtube video embedded in the update"
                                video_count += 1
                        
                        # if not clickable, also say that this is a gif
                        except ElementClickInterceptedException:
                            gif_counter += 1
                        except ElementNotInteractableException:
                            gif_counter += 1
                    
                    sleep(5)
                    
                    new_dict = {'ID': update_id,
                                'Name': name,
                                'AuthorTitle': title_name,
                                'Time': time,
                                'OnlyVisibleForBackers': only_visible_for_backers,
                                'LikesCount': like_count,
                                'CommentCount': comment_count,
                                'UpdateTitle': update_title,
                                'PictureCount': pic_counter,
                                'GifCount': gif_counter,
                                'VideoCount': video_count,
                                'Content': update_text}
                    
                    update_list.append(new_dict)
                
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                
                # case that it is a hidden update
                if child.find_elements_by_xpath(".//div[2]/div/article/div/h3"):
                    print("now crawling children: ", idx, " which is ONLY VISIBLE for backers. of project: ", pr_nr)
                    
                    only_visible_for_backers = True 
                    
                    update_id = child.find_element_by_xpath(".//div[2]/div/article/header/div[1]/div/span[1]").text    # div[2]/div/article/header/div[1]/div/span[1]
                    update_title = child.find_element_by_xpath(".//div[2]/div/article/header/h2").text
                    #print("the update title is: ", update_title)
                    
                    comment_count = child.find_element_by_xpath(".//div[2]/div/article/footer/div/div/span[1]").text
                    #print("comment count is: ", comment_count)
                    
                    like_count = child.find_element_by_xpath(".//div[2]/div/article/footer/div/div/span[2]").text
                    #print("like count is: ", like_count)
                    
                    personal_info = child.find_element_by_xpath(".//div[2]/div/article/header/div[2]/div[2]/div")
                    raw_name = personal_info.text
                    title_name = personal_info.find_element_by_xpath(".//span").text
                    name = raw_name.replace(title_name, '')
                    
                    time = child.find_element_by_xpath(".//div[2]/div/article/header/div[2]/div[2]/span").text
                    
                    new_dict = {'ID': update_id,
                                'Name': name,
                                'AuthorTitle': title_name,
                                'Time': time,
                                'OnlyVisibleForBackers': only_visible_for_backers,
                                'LikesCount': like_count,
                                'CommentCount': comment_count,
                                'UpdateTitle': update_title,
                                'PictureCount': np.nan,
                                'GifCount': np.nan,
                                'VideoCount': np.nan,
                                'Content': np.nan}
                    
                    
                    only_visible_for_backers_count += 1
                    
                    update_list.append(new_dict)
                    
                    
            my_item_func = lambda x: 'update'
            
            test_xml = dicttoxml(update_list, 
                                 custom_root="updates", 
                                 attr_type=False,
                                 item_func=my_item_func)
            dom = parseString(test_xml)        
            
            #test_pretty_xml = dom.toprettyxml()

        driver.close()                
        
        if not project_hidden:
            with open(path + "/" + "update_" + pr_nr + ".xml", mode="w", encoding="utf-8") as f:
                    f.write(dom.toprettyxml())
        

    except Exception as e:
        print("Error -> Mark Link as Error in Database")
        print(e)
        return


    return faq, only_visible_for_backers_count, project_hidden


#%% calling the function
    
if __name__ == "__main__":
    
    metadata_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Python_Analysis/data/overview_files"
    overview_file = pd.read_csv(os.path.join(metadata_path, "current_update_metadata.csv"))
    
    for row in overview_file.itertuples(index=True, name="Project"):
        
        if row.updates_downloaded == False:  # only download projects that have not been downloaded before
            faq_count, only_backers_visible_count, hidden_project = crawl_updates(
                link=row.Link,
                pr_nr=row.Project_Nr,
                path=row.save_path)
            
            overview_file.at[row.Index, "updates_downloaded"] = True
            overview_file.at[row.Index, "faq"] = faq_count
            overview_file.at[row.Index, "updates_only_visible_for_backers"] = only_backers_visible_count
            overview_file.at[row.Index, "hidden_project"] = hidden_project

    overview_file.to_csv(os.path.join(metadata_path, "current_update_metadata.csv"), index=False)

#%% this is only here for archive reasons to know how to run the script individually without overview file
"""

save_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML_local_download/art/Updates"

get_all_input(link="https://www.kickstarter.com/projects/1039804339/ben-newmans-matador",
              path = save_path,
              pr_nr = "1039804339")

"""



'''

this is button load more in updates:
    
//*[@id="project-post-interface"]/div/div[11]/div/div/div/button
//*[@id="project-post-interface"]/div/div[21]/div/div/div/button

--> can use last() in xpath instead of 11 or 21
    
this is button load more in comments:
//*[@id="react-project-comments"]/div/button



'''


