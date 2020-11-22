from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
# from bs4 import BeautifulSoup
# import re
# from sqlalchemy import create_engine
import pandas as pd
import requests
from time import sleep
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
from datetime import datetime

overview_file = pd.read_csv("/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML Info/art_xml_tobescraped.csv")
overview_file["downloaded"] = False
overview_file["error_description"] = ""
overview_file["withdrawn_comments"] = ""

link1 = "https://www.kickstarter.com/projects/171804600/the-art-of-eddie-nunez?ref=discovery_category_popular"
link2 = "https://www.kickstarter.com/projects/nicholascladis/a-land-of-narrow-paths?ref=discovery_category_popular"

new_l = link2.split("?")[0]
new_link = new_l + "/comments"

response = requests.get(new_link)

options = webdriver.ChromeOptions()
options.add_argument("headless")
driver = webdriver.Chrome(executable_path="/Users/philippsach/.conda/envs/Python_Analysis/bin/chromedriver", options=options)
driver.set_window_position(2000,0)
driver.delete_all_cookies()
driver.get(new_link)
driver.switch_to.default_content()

# click the button to load more comments
while True:
    try:
        sleep(2)
        showmore = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
            (By.XPATH, '//*[@id="react-project-comments"]/div/button')))
        showmore.click()



    except TimeoutException:
        break
    except StaleElementReferenceException:
        break

# click the button to load more replies if there are more than three replies to a comment
while True:
    try:
        sleep(2)
        showmoreanswers = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
            (By.XPATH, '//*[@id="react-project-comments"]/ul/li/div/div/button')))
        # showmoreanswers.click()
        driver.execute_script("arguments[0].click();", showmoreanswers)
    
    except TimeoutException:
        break
    except StaleElementReferenceException:
        break



if not driver.find_element_by_id("react-project-comments"):
    driver.close()


iframe = driver.find_element_by_id("react-project-comments")

comment_dict = {}
all_comments = iframe.find_elements_by_xpath("// *[ @ id = \"react-project-comments\"] / ul / li")
#all_comments_css = iframe.find_elements_by_css_selector("#react-project-comments > ul > li")

comment_count = 0
withdrawn_count = 0

for comment in all_comments:
    if not comment.find_elements_by_class_name("flex"):
        break
    comment_count += 1
    personal_info = comment.find_elements_by_class_name("flex")[0]
    if not personal_info.find_elements_by_tag_name("span"):
        print("Comment corrupted --> Backer withdraw")
        withdrawn_count += 1
        continue
    name = personal_info.find_elements_by_tag_name("span")[0].text
    print(name)
    title = personal_info.find_elements_by_tag_name("span")[1]
    if title:
        title_name = title.text
        # print(title_name)
    id = personal_info.find_element_by_class_name("comment-link").get_attribute("href")
    id = id.split('?')[1]
    # print(id)
    content = comment.find_elements_by_class_name("flex")[2].text
    # print(content)
    time = personal_info.find_element_by_tag_name("time").get_attribute("title")
    # print(time)
    new_dict = {'ID': id, 'Name': name, 'Title': title_name, 'Time': time,
                    'Content': content}
    # answers = comment.find_element_by_class_name("pl6 pt2")
    all_answers = comment.find_elements_by_tag_name("li")
    answer_count = 0
    # print(len(all_answers))
    for answer in all_answers:
        answer_count += 1
        personal_info_answer = answer.find_elements_by_class_name("flex")[0]
        if not personal_info_answer.find_elements_by_tag_name("span"):
            print("Comment corrupted --> Backer withdraw")
            withdrawn_count += 1
            continue
        answer_name = personal_info_answer.find_elements_by_tag_name("span")[0].text
        answer_title = personal_info_answer.find_elements_by_tag_name("span")[1]
        if answer_title:
            answer_title_name = answer_title.text
        answer_id = personal_info_answer.find_element_by_class_name("comment-link").get_attribute("href")
        answer_id = answer_id.split('?')[1]
        answer_content = answer.find_elements_by_class_name("flex")[2].text
        answer_time = personal_info_answer.find_element_by_tag_name("time").get_attribute("title")
        new_dict.update({'Answer ' + str(answer_count): {'ID': answer_id, 'Name': answer_name,
                                                         'Title': answer_title_name, 'Time': answer_time,
                                                         'Content': answer_content}})
    comment_dict.update({"Comment " + str(comment_count): new_dict})
    # print(new_dict)

driver.close()

xml = dicttoxml(comment_dict, custom_root='comments', attr_type=False)
dom = parseString(xml)

#with open ("/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML Info/" + "comment_171804600.xml", mode="w", encoding="utf-8") as f:
 #   f.write(dom.toprettyxml())

with open ("/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML Info/" + "comment_nicholascladis.xml", mode="w", encoding="utf-8") as f:
   f.write(dom.toprettyxml())

test_dataframe = pd.DataFrame.from_dict(comment_dict, orient = "index")


def crawl_comments(link, pr_nr, path):
    print("Now crawling comments")
    new_l = link.split("?")[0]
    new_link = new_l + "/comments"
    
    options = webdriver.ChromeOptions()
    options.add_argument("headless")
    driver = webdriver.Chrome(executable_path="/Users/philippsach/.conda/envs/Python_Analysis/bin/chromedriver", options=options)
    driver.set_window_position(2000,0)
    driver.delete_all_cookies()
    driver.get(new_link)
    driver.switch_to.default_content()
    
    # click the button to load more comments
    while True:
        try:
            sleep(2)
            showmore = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                (By.XPATH, '//*[@id="react-project-comments"]/div/button')))
            showmore.click()

        except TimeoutException:
            break
        except StaleElementReferenceException:
            break
    
    # click the button to load more replies if there are more than three replies to a comment
    while True:
        try:
            sleep(2)
            showmoreanswers = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                (By.XPATH, '//*[@id="react-project-comments"]/ul/li/div/div/button')))
            # showmoreanswers.click()
            driver.execute_script("arguments[0].click();", showmoreanswers)
        
        except TimeoutException:
            break
        except StaleElementReferenceException:
            break
    
    if not driver.find_element_by_id("react-project-comments"):
        driver.close()

    iframe = driver.find_element_by_id("react-project-comments")
    
    comment_dict = {}
    all_comments = iframe.find_elements_by_xpath("// *[ @ id = \"react-project-comments\"] / ul / li")
    #all_comments_css = iframe.find_elements_by_css_selector("#react-project-comments > ul > li")
    
    comment_count = 0
    withdrawn_count = 0
    
    for comment in all_comments:
        if not comment.find_elements_by_class_name("flex"):
            break

        comment_count += 1
        personal_info = comment.find_elements_by_class_name("flex")[0]

        if not personal_info.find_elements_by_tag_name("span"):
            print("Comment corrupted --> Backer withdraw")
            withdrawn_count += 1
            continue

        name = personal_info.find_elements_by_tag_name("span")[0].text
        print(name)
        title = personal_info.find_elements_by_tag_name("span")[1]

        if title:
            title_name = title.text
            # print(title_name)

        id = personal_info.find_element_by_class_name("comment-link").get_attribute("href")
        id = id.split('?')[1]
        # print(id)

        content = comment.find_elements_by_class_name("flex")[2].text
        # print(content)

        time = personal_info.find_element_by_tag_name("time").get_attribute("title")
        # print(time)

        new_dict = {'ID': id, 'Name': name, 'Title': title_name, 'Time': time,
                        'Content': content}

        # answers = comment.find_element_by_class_name("pl6 pt2")
        all_answers = comment.find_elements_by_tag_name("li")
        answer_count = 0
        # print(len(all_answers))

        for answer in all_answers:
            answer_count += 1
            personal_info_answer = answer.find_elements_by_class_name("flex")[0]

            if not personal_info_answer.find_elements_by_tag_name("span"):
                print("Comment corrupted --> Backer withdraw")
                withdrawn_count += 1
                continue

            answer_name = personal_info_answer.find_elements_by_tag_name("span")[0].text
            answer_title = personal_info_answer.find_elements_by_tag_name("span")[1]

            if answer_title:
                answer_title_name = answer_title.text

            answer_id = personal_info_answer.find_element_by_class_name("comment-link").get_attribute("href")
            answer_id = answer_id.split('?')[1]

            answer_content = answer.find_elements_by_class_name("flex")[2].text
            answer_time = personal_info_answer.find_element_by_tag_name("time").get_attribute("title")

            new_dict.update({'Answer ' + str(answer_count): {'ID': answer_id, 'Name': answer_name,
                                                             'Title': answer_title_name, 'Time': answer_time,
                                                             'Content': answer_content}})

        comment_dict.update({"Comment " + str(comment_count): new_dict})
        # print(new_dict)
    
    driver.close()
    
    xml = dicttoxml(comment_dict, custom_root='comments', attr_type=False)
    dom = parseString(xml)

    with open(path + "/" + "comment_" + pr_nr + ".xml", mode="w", encoding="utf-8") as f:
        f.write(dom.toprettyxml())

    return withdrawn_count
    

def get_all_input(link, pr_nr, path):
    try:
        print("=============================================================")
        print("Time: " + str(datetime.now()))
        response = requests.get(link)
        if response.status_code == 200:
            print(response.status_code)
            # soup = BeautifulSoup(response.text, "lxml")
        else:
            # print("CONNECTION ERROR: " + str(response.status_code))
            if response.status_code == 429:
                print("TIMEOUT, OVERLOAD")
                time.sleep(60)
                print("Retry last link")
                response = requests.get(link)
                # print(response.status_code)
                if response.status_code != 200:
                    print("SOMETHING WENT WRONG --> Wait another interval")
                    time.sleep(60)
                    response = requests.get(link)
                    print(response.status_code)
                # soup = BeautifulSoup(response.text, "lxml")
            else:
                print("UNKNOWN ERROR - DO SOMETHING")
                print("<--------------------->")
                return(0, float("NaN"))
                
    except Exception as e:
        print("Error -> Mark Link as Error in Database")
        print(e)
        return(0, float("NaN"))
    
    withdrawn_count = crawl_comments(link=link,
                                     pr_nr=pr_nr, 
                                     path=path)

    return(1, withdrawn_count)


def wrapper_function(path):
    print("let the crawling begin")
    for row in overview_file.loc[50:55].itertuples(index=True, name="Project"):
        # overview_file.at[row.Index, "downloaded"] = False
        print(row.Project_Nr)
        print(row.Link)
        error_code, withdrawn_count = get_all_input(link=row.Link, pr_nr = row.Project_Nr, path = path)
        
        # problem occured, mark this in our row!!
        if error_code==0:
            overview_file.at[row.Index, "error_description"] = "accessing the link"
        elif error_code == 100:
            print("this is a new error type I can define here")
        else:
            overview_file.at[row.Index, "downloaded"] = True
            overview_file.at[row.Index, "withdrawn_comments"] = withdrawn_count

# save_path = "/Users/philippsach/HiDrive/public/Kickstarter_Data/art/Comments"
save_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML Info"

wrapper_function(path=save_path)
