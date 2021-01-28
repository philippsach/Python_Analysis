from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import pychrome
from fake_useragent import UserAgent
import pandas as pd
import numpy as np
import requests
from time import sleep
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
from datetime import datetime
import multiprocessing as mp
import os
import urllib3
import lxml.etree as et

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


mp.set_start_method('spawn', force=True)
__spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
#print(mp.get_start_method())

category_data = {
    "category": ["art", "comic", "craft", "dance", "design",
                 "fashion", "food", "games", "journalism", "music",
                 "photo", "publishing", "technology", "theater", "video"]
}

category_df = pd.DataFrame(category_data)


# mac path
category_df["path"] = category_df.apply(
    lambda x: "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML_local_download/" + 
    x["category"] + 
    "/Comments", 
    axis = 1)

category = category_df.iloc[12,0]
save_path = category_df.iloc[12,1]

if os.path.isdir("Python_Analysis"):  # then  we are on the windows laptop
    overview_base_path = "Python_Analysis"
else:
    overview_base_path = ""

overview_path = overview_base_path + "../data/overview_files/current_" + category + "_metadata.csv"
overview_file = pd.read_csv(overview_path)
overview_file["error_description"] = overview_file["error_description"].astype("string")
overview_file = overview_file.sort_values(by="comments")

# only process projects with less than 1500 comments in a parallelized manner
# the rest needs to be taken care of separately
# overview_file = overview_file[overview_file["comments"]<=1500]

# print(overview_file.dtypes)
print("juhuu outside")

# TODO: implement the following:
# Click and hold the button if : "Verify that you are a human"
# https://stackoverflow.com/questions/8787830/click-and-hold-with-webdriver

def crawl_comments(link, pr_nr, path, n_comments_sql):
    print("Now crawling comments")
    new_l = link.split("?")[0]
    new_link = new_l + "/comments"
    
    options = webdriver.ChromeOptions()
    
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    options.add_argument("--disable-blink-features")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    options.add_argument("window-size=1280,800")
    
    # options.add_argument("--incognito")  # better not use incognito... 
    
    # options.add_argument("headless")
    
    # options.add_argument("--remote-debugging-port=8000")  # DEVTOOLS
    
    
    # ua = UserAgent()
    # userAgent = ua.random
    # print("following user agent is used: ", userAgent)
    # options.add_argument(f"user-agent={userAgent}")
    
    options.add_argument("Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36")
    
    # options.add_argument("--enable-javascript")
    # options.add_argument("--no-sandbox")
    # options.add_argument("plugins=1")
    # options.add_argument("languages=en-gb")
    driver = webdriver.Chrome(executable_path="/Users/philippsach/.conda/envs/Python_Analysis/bin/chromedriver_copy",
                              options=options)
    
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    # DEVTOOLS
    '''
    dev_tools = pychrome.Browser(url="http://localhost:8000")  
    tab = dev_tools.list_tab()[0]  
    tab.start() 
    '''
    
    # driver.set_window_position(2000, 0)
    driver.delete_all_cookies()
    driver.get(new_link)
    driver.switch_to.default_content()
    
    # try to catch the case when automatic scraping was detected
    if driver.find_elements_by_class_name("page-title"):
        if driver.find_element_by_class_name("page-title").text == "Please verify you are a human":
            print("mierda, we got blown")
            sleep(10)  # this time is needed for manual clicking of PRESS AND HOLD
            input("Press ENTER to continue ... ")
    
    if n_comments_sql > 20:
        # click the button to load more comments
        while True:
            try:
                sleepTime = np.random.uniform(1.5,4)
                sleep(sleepTime)
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
                sleepTime = np.random.uniform(1, 3)
                sleep(sleepTime)
                showmoreanswers = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="react-project-comments"]/ul/li/div/div/button')))
                # showmoreanswers.click()
                driver.execute_script("arguments[0].click();", showmoreanswers)

            except TimeoutException:
                break
            except StaleElementReferenceException:
                break
    
    # if project is a hidden project (like https://www.kickstarter.com/projects/1372947955/build-your-paradise for example)
    # then break out of here
    if driver.find_elements_by_id("hidden_project"):
        print("hidden project")
        error_code = 20
        withdrawn_count = float("NaN")
        driver.close()
    
    
    else:
    
        if not driver.find_elements_by_id("react-project-comments"):
            print("haven't found react-project-comments")
            error_code = 30
            sleepTime = np.random.uniform(2,3)  # need to press and hold manually myself during that time
            sleep(sleepTime)
            withdrawn_count = float("NaN")
            driver.close()
        
        else:   
    
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
            
            error_code = 1

    return error_code, withdrawn_count
    

def get_all_input(link, pr_nr, path, n_comments_sql):
    try:
        print("=============================================================")
        print("Time: " + str(datetime.now()))
        print("Link: " + link)
        sleep(5)
        
        # session = requests.Session() # new line
        # retry = requests.packages.urllib3.util.retry.Retry(
        #     connect=3,
        #     status_forcelist=[61, 429, 500, 502, 503, 504],
        #     method_whitelist=["HEAD", "GET", "OPTIONS"],
        #     backoff_factor=2)
        # adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        # session.mount("http://", adapter)
        # session.mount("https://", adapter)
        # response = session.get(link, verify=False)
        
        response = requests.get(link, verify=False)
        
        if response.status_code == 200:
            print(response.status_code)
            # soup = BeautifulSoup(response.text, "lxml")
        else:
            # print("CONNECTION ERROR: " + str(response.status_code))
            if response.status_code == 429:
                print("TIMEOUT, OVERLOAD")
                sleep(60)
                print("Retry last link")
                response = requests.get(link, verify=False)
                # print(response.status_code)
                if response.status_code != 200:
                    print("SOMETHING WENT WRONG --> Wait another interval")
                    sleep(60)
                    response = requests.get(link, verify=False)
                    print(requests.status_code)
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
    for row in overview_file.itertuples(index=True, name="Project"):
        # overview_file.at[row.Index, "downloaded"] = False
        if row.downloaded == False:  # only download projects that have not been downloaded before
            error_code, withdrawn_count = get_all_input(
                link=row.Link,
                pr_nr=row.Project_Nr,
                path=path,
                n_comments_sql=row.comments
                )
        
            # problem occured, mark this in our row!!
            if error_code == 10:
                overview_file.at[row.Index, "error_description"] = "accessing the link"
            if error_code == 20:
                overview_file.at[row.Index, "error_description"] = "hidden project"
            if error_code == 30:
                overview_file.at[row.Index, "error_description"] = "no react-project-comments"
            else:
                overview_file.at[row.Index, "downloaded"] = True
                overview_file.at[row.Index, "withdrawn_comments_new"] = withdrawn_count
            

def new_get_all_input(row):
    sleep(2)
    link = row[3]
    pr_nr = row[0]
    path = save_path
    n_comments_sql = row[1]
    try:
        print("=============================================================")
        print("Time: " + str(datetime.now()))
        session = requests.Session() # new line
        retry = requests.packages.urllib3.util.retry.Retry(
            total=3,
            status_forcelist=[61, 429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"],
            backoff_factor=2)
        adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        response = session.get(link, verify=False)
        #response = requests.get(link, verify=False)
        if response.status_code == 200:
            print(response.status_code)
            # soup = BeautifulSoup(response.text, "lxml")
        else:
            # print("CONNECTION ERROR: " + str(response.status_code))
            if response.status_code == 429:
                print("TIMEOUT, OVERLOAD")
                sleep(60)
                print("Retry last link")
                response = requests.get(link, verify=False)
                # print(response.status_code)
                if response.status_code != 200:
                    print("SOMETHING WENT WRONG --> Wait another interval")
                    sleep(60)
                    response = requests.get(link, verify=False)
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

    return pr_nr, error_code, withdrawn_count

# testing it in sequential way....
wrapper_function(path=save_path)



overview_file.to_csv(overview_base_path + "../data/overview_files/current_" + category + "_metadata.csv", index=False)
print("finished successfully and saved to file")
# testing the new setup <3
# if __name__ == "__main__":
#     num_processes = mp.cpu_count()
#     pool = mp.Pool(num_processes)
#     result_1000 = pool.map(new_get_all_input, overview_file.iloc[0:1000].itertuples(index=False, name=None), chunksize=2)
#     result_2000 = pool.map(new_get_all_input, overview_file.iloc[1001:2000].itertuples(index=False, name=None), chunksize=2)
#     result_3000 = pool.map(new_get_all_input, overview_file.iloc[2001:3000].itertuples(index=False, name=None), chunksize=2)
#     result_4000 = pool.map(new_get_all_input, overview_file.iloc[3001:4000].itertuples(index=False, name=None), chunksize=2)
#     result_5000 = pool.map(new_get_all_input, overview_file.iloc[4001:5000].itertuples(index=False, name=None), chunksize=2)
#     result_6000 = pool.map(new_get_all_input, overview_file.iloc[5001:].itertuples(index=False, name=None), chunksize=2)
    #result = pd.DataFrame(result, columns=["Project_Nr", "error_code", "withdrawn_count"])
    
# checking how long it takes with old functions in sequence
# wrapper_function(path=save_path)


# just testing some random stuff, e.g. with wrong url's what happens etc.
#get_all_input("https://www.kickstarter.com/projects/arten2/the-art-of-eddie-nunes", pr_nr = "NUNEZZZZ", path=save_path )
