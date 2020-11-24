from selenium import webdriver
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

# old transformation steps for reference
# overview_file = overview_file[overview_file["comments"] > 0]
# overview_file.to_csv("/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML Info/art_xml_tobescraped_current_status.csv", index = False)


overview_file = pd.read_csv("/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML Info/art_xml_tobescraped_current_status.csv")
overview_file["error_description"] = overview_file["error_description"].astype("string")
print(overview_file.dtypes)

# overview_file_test = overview_file[overview_file["comments"] > 0]  # ONLY NEEDED BECAUSE I DID IT WRONG BEFORE

def crawl_comments(link, pr_nr, path, n_comments_sql):
    print("Now crawling comments")
    new_l = link.split("?")[0]
    new_link = new_l + "/comments"
    
    options = webdriver.ChromeOptions()
    options.add_argument("headless")
    driver = webdriver.Chrome(executable_path="/Users/philippsach/.conda/envs/Python_Analysis/bin/chromedriver",
                              options=options)
    driver.set_window_position(2000, 0)
    driver.delete_all_cookies()
    driver.get(new_link)
    driver.switch_to.default_content()

    if n_comments_sql > 20:
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

    if n_comments_sql > 4:
        # can only happen if there are at least 5 comments in total (1 comment + 4 replies of which only first 3 are shown)
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
    # all_comments_css = iframe.find_elements_by_css_selector("#react-project-comments > ul > li")
    
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

        new_dict = {'ID': id,
                    'Name': name,
                    'Title': title_name,
                    'Time': time,
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

    return 1, withdrawn_count
    

def get_all_input(link, pr_nr, path, n_comments_sql):
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
                sleep(60)
                print("Retry last link")
                response = requests.get(link)
                # print(response.status_code)
                if response.status_code != 200:
                    print("SOMETHING WENT WRONG --> Wait another interval")
                    sleep(60)
                    response = requests.get(link)
                    print(response.status_code)
                # soup = BeautifulSoup(response.text, "lxml")
            else:
                print("UNKNOWN ERROR - DO SOMETHING")
                print("<--------------------->")
                return 10, float("NaN")
                
    except Exception as e:
        print("Error -> Mark Link as Error in Database")
        print(e)
        return 10, float("NaN")
    
    error_code, withdrawn_count = crawl_comments(link=link,
                                                 pr_nr=pr_nr,
                                                 path=path,
                                                 n_comments_sql=n_comments_sql)

    return error_code, withdrawn_count


def wrapper_function(path):
    print("let the crawling begin")  # by now, have not tested project 1. also, now crawling last 1000 to see how long it takes
    for row in overview_file.loc[5000:].itertuples(index=True, name="Project"):
        # overview_file.at[row.Index, "downloaded"] = False
        print(row.Project_Nr)
        print(row.Link)
        error_code, withdrawn_count = get_all_input(
            link=row.Link,
            pr_nr=row.Project_Nr,
            path=path,
            n_comments_sql=row.comments
            )
        
        # problem occured, mark this in our row!!
        if error_code == 10:
            overview_file.at[row.Index, "error_description"] = "accessing the link"
        else:
            overview_file.at[row.Index, "downloaded"] = True
            overview_file.at[row.Index, "withdrawn_comments"] = withdrawn_count


# save_path = "/Users/philippsach/HiDrive/public/Kickstarter_Data/art/Comments"
save_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML Info"

wrapper_function(path=save_path)


# just testing some random stuff, e.g. with wrong url's what happens etc.
get_all_input("https://www.kickstarter.com/projects/arten2/the-art-of-eddie-nunes", pr_nr = "NUNEZZZZ", path=save_path )
