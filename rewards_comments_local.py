from bs4 import BeautifulSoup, NavigableString, Tag
import bs4
import requests
import mysql.connector
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import mysql
import json
import time
import random
from dicttoxml import dicttoxml
import os
import re
from selenium import webdriver
import csv
from datetime import datetime
from xml.dom.minidom import parseString

list_of_categories_art = [287, 20, 21, 22, 288, 54, 23, 24, 53, 25, 289, 290, 395]
list_of_categories_comic = [249, 250, 251, 252, 253]
list_of_categories_craft = [343, 344, 345, 346, 347, 348, 350, 351, 352, 353, 354, 355, 356]
list_of_categories_dance = [254, 255, 256, 257]
list_of_categories_design = [258, 259, 27, 260, 28, 261]
list_of_categories_fashion = [262, 263, 264, 265, 266, 267, 268, 269]
list_of_categories_food = [304, 305, 306, 307, 308, 310, 309, 311, 312, 313, 314, 315]
list_of_categories_games = [270, 271, 272, 273, 274]
#Already done in Video:
# 291, 29, 292, 30, 293, 294, 330, 296, 295, 297, 298, 299,
list_of_categories_video = [ 31, 300, 301, 32, 303, 302, 33]
list_of_categories_journalism = [357, 358, 359, 360, 361]
list_of_categories_music = [316, 317, 36, 386, 37, 38, 318, 39, 40, 41, 319, 320, 241, 42, 321, 322, 43, 44]
list_of_categories_photo = [275, 276, 277, 278, 280, 279]
list_of_categories_publishing = [323, 324, 45, 325, 46, 387, 47, 349, 326, 48, 49, 50, 239, 327, 328, 329, 389]
list_of_categories_technology = [331, 332, 333, 334, 335, 336, 337, 52, 362, 338, 51, 339, 340, 341, 342]
list_of_categories_theater = [388, 281, 282, 283, 284, 285, 286]


# List of all Top Categories missing
# TODO: Change Categories HERE!
# Technology is already done so no need to add it here
list_of_top_cat = [list_of_categories_video, list_of_categories_photo, list_of_categories_art,
                      list_of_categories_comic, list_of_categories_craft, list_of_categories_dance,
                      list_of_categories_design, list_of_categories_fashion, list_of_categories_food,
                      list_of_categories_games, list_of_categories_journalism, list_of_categories_music,
                      list_of_categories_publishing, list_of_categories_theater]

# TODO: Change Folder HERE
top_folder = "CHANGE HERE"
folder_of_comments = "Comments/"
folder_of_rewards = "Rewards/"

# TODO: Change Database Metadata here
mysql_ip = "85.214.204.221"
mysql_user = "uwe"
mysql_pass = "uwe"


def getRelevant(name):
    if name == "id" or name == "pid" or name == "name" or name == "currency" or name == "description" or \
            name == "isProjectWeLove" or name == "category" or name == "location" or name == "curatedCollection" \
            or name == "backersCount" or name == "percentFunded" or name == "goal" or name == "pledged" or \
            name == "creator" or name == "deadlineAt" or name == "duration" or name == "state" or name == "canceledAt":
        return True


def crawl(soup):
    if soup.find("div", {'class': 'bg-grey-100'}) is not None:
        data = soup.find("div", {'class': 'bg-grey-100'}).get('data-initial')
    else:
        if soup.find("div", {'class': 'Campaign-state-successful'}) is not None:
            # print("Successful project")
            data = ''
    return data


def comments(link, pr_nr, folder_of_data):
    # Muster: <Kommentar> <ID> <Name> <Titel> <Inhalt> <Antwort_X>
    print("Now Crawling Comments")
    new_l = link.split("?")[0]
    new_link = new_l + "/comments"
    # print(new_link)
    options = webdriver.ChromeOptions()
    options.add_argument("headless")
    browser = webdriver.Chrome('chromedriver.exe', options=options)
    browser.set_window_position(2000, 0)
    browser.delete_all_cookies()
    browser.get(new_link)
    browser.switch_to.default_content()
    if not browser.find_element_by_id("react-project-comments"):
        browser.close()
        return 0
    iframe = browser.find_element_by_id("react-project-comments")
    comment_dict = {}
    all_comments = iframe.find_elements_by_xpath("// *[ @ id = \"react-project-comments\"] / ul / li")
    # print(len(all_comments))
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
        # print(name)
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
    browser.close()
    # print(comment_dict)
    xml = dicttoxml(comment_dict, custom_root='comments', attr_type=False)
    dom = parseString(xml)
    with open(folder_of_data + folder_of_comments + "comment_" + str(pr_nr) + ".xml", mode="w", encoding='utf-8') as f:
        f.write(dom.toprettyxml())
    return withdrawn_count


def rewards(soup, pr_nr, folder_of_data):
    print("Now Crawling Rewards")
    rewards = soup.find_all("div", {"class": "pledge__info"})
    all_rewards = []
    reward_dict = {}
    reward_counter = 0
    #print("Test")
    for reward in rewards:
        # Muster: <Reward> <Titel> <Betrag> <Text> <Versand> <Lieferung> <Begrenzt> <Verfügbar> <Anzahl Unterstützer>
        if reward.find("div", {"class": "pledge__backer-stats"}):
            amount = reward.find("h2", {"class": "pledge__amount"}).text
            titel = ''
            if reward.find("h3", {"class": "pledge__title"}):
                titel = reward.find("h3", {"class": "pledge__title"}).text
            text = reward.find("div", {"class": "pledge__reward-description pledge__reward-description--expanded"}).text
            extra_info = reward.find("div", {"class": "pledge__extra-info"}).text
            backer = reward.find("div", {"class": "pledge__backer-stats"}).text
            reward_counter += 1
            reward_dict.update({'Reward ' + str(reward_counter): {'Title': titel, 'Amount': amount, 'Text': text,
                                                             'Extra Info': extra_info, 'Backers': backer}})
        all_rewards.append(reward.text)
    xml = dicttoxml(reward_dict, custom_root='rewards', attr_type=False)
    #print(reward_dict)
    dom = parseString(xml)
    with open(folder_of_data + folder_of_rewards + "reward_" + str(pr_nr) + ".xml", mode="w", encoding="utf-8") as f:
        f.write(dom.toprettyxml())
    return reward_counter, all_rewards


def identify_category(cat_id):
    if cat_id in list_of_categories_theater:
        return "theater"
    elif cat_id in list_of_categories_publishing:
        return "publishing"
    elif cat_id in list_of_categories_music:
        return "music"
    elif cat_id in list_of_categories_journalism:
        return "journalism"
    elif cat_id in list_of_categories_games:
        return "games"
    elif cat_id in list_of_categories_food:
        return "food"
    elif cat_id in list_of_categories_fashion:
        return "fashion"
    elif cat_id in list_of_categories_design:
        return "design"
    elif cat_id in list_of_categories_dance:
        return "dance"
    elif cat_id in list_of_categories_craft:
        return "craft"
    elif cat_id in list_of_categories_comic:
        return "comic"
    elif cat_id in list_of_categories_technology:
        return "technology_new"
    elif cat_id in list_of_categories_art:
        return "art"
    elif cat_id in list_of_categories_video:
        return "video"
    elif cat_id in list_of_categories_photo:
        return "photo"


def get_all_input(cat_id, status, count):
    print("§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§§")
    main_category = identify_category(cat_id)
    sql = "Select link from thesis." + str(main_category) + " where reward_comment_xml = False " \
                                                            "or reward_comment_xml = null " \
                                                            " LIMIT 1000;"
    cnx = mysql.connector.connect(user=mysql_user, password=mysql_pass, host=mysql_ip, database='thesis')
    cursor = cnx.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    lines = []
    for x in result:
        lines.append(x[0])
    print("Anzahl der mindestens zu crawlenden Projekte: " + str(len(lines)))
    cnx.commit()
    cursor.close()
    cnx.close()
    if len(lines) == 0:
        print("All Links for this sub_category with this status were crawled --> Abort")
        return True
    if len(lines) < 10:
        print("Less than 10 Links in the list, take whole list")
        random_lines = lines
    else:
        print("Get 10 Random Links from List")
        random_lines = random.sample(lines, 10)
    #print(random_lines)
    print("Main Category: " + str(main_category))
    folder_of_data = top_folder + str(main_category) + '/'
    tablename = str(main_category)
    for line in random_lines:
        try:
            print("=============================================================")
            print("Time: " + str(datetime.now()))
            print(line)
            response = requests.get(line)
            if response.status_code == 200:
                #print(response.status_code)
                soup = BeautifulSoup(response.text, "lxml")
            else:
                #print("CONNECTION ERROR: " + str(response.status_code))
                if response.status_code == 429:
                    print("TIMEOUT, OVERLOAD")
                    time.sleep(60)
                    print("Retry last link")
                    response = requests.get(line)
                    #print(response.status_code)
                    if response.status_code != 200:
                        print("SOMETHING WENT WRONG --> Wait another interval")
                        time.sleep(60)
                        response = requests.get(line)
                        print(response.status_code)
                    soup = BeautifulSoup(response.text, "lxml")
                else:
                    print("UNKNOWN ERROR - DO SOMETHING")
                    print("<--------------------->")
            #print("CATEGORY ID:" + str(cat_id))
            hidden = soup.find("div", {"id": "hidden_project"})
            if hidden is not None:
                if hidden.h2.text is not None:
                    print("Project was hidden -- Next project")
                    continue
            data = crawl(soup)
            pid = ''
            values = {}
            j_data = json.loads(data)
            project_info = j_data['project']
            for x in project_info:
                if getRelevant(x):
                    if x == 'pid':
                        pid = str(project_info[x])
                    values.update({str(x): project_info[x]})
            if data == '':
                k = 0
                while k < 6:
                    time.sleep(2)
                    pid = line.split("/")[4]
            print(values["state"])
            withdrawn_count = comments(line, pid, folder_of_data)
            reward_counter, all_rewards = rewards(soup, pid, folder_of_data)
            sql_1 = "Update thesis." + tablename + " Set reward_comment_xml = True where link = \"" + str(line) + "\";"
            cnx = mysql.connector.connect(user=mysql_user, password=mysql_pass, host=mysql_ip, database='thesis')
            cursor = cnx.cursor()
            cursor.execute(sql_1)
            cnx.commit()
            cursor.close()
            cnx.close()

        except Exception as e:
            print("Error -> Mark Link as Error in Database")
            print(e)
            sql_1 = "Update thesis." + tablename + " Set reward_comment_xml = False where link = \""+str(line)+"\";"
            cnx = mysql.connector.connect(user=mysql_user, password=mysql_pass, host=mysql_ip, database='thesis')
            cursor = cnx.cursor()
            cursor.execute(sql_1)
            cnx.commit()
            cursor.close()
            cnx.close()


def wrapper_function():
    print("Only Comments and Rewards are Crawled")
    xml_crawl = False
    for cat in list_of_top_cat:
        for cat_id in cat:
            print("CATEGORY ID is: " + str(cat_id))
            status = "not touched"
            count = "0"
            print("Get all Links which have no rewards or comments")
            while True:
                value = get_all_input(cat_id, status, count)
                if value:
                    break
            print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
            print("Finished all Projects which were not touched")
            print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    print("Category Finished")


wrapper_function()