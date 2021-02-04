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
        
        while True:
            try:
                showmore=WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH ,'//*[@id="project-post-interface"]/div/div[11]/div/div/div/button')))
                showmore.click()
            except TimeoutException:
                break
            except StaleElementReferenceException:
                break
        
        
        #soup = BeautifulSoup(driver.page_source, "lxml")
        
        # extract the FAQ count
        faq = driver.find_element_by_css_selector("#faq-emoji").get_attribute("emoji-data")
        #print("faq is: ", faq)
        
        children = driver.find_elements_by_css_selector('#project-post-interface > div > div > div >div > a[href]')

        update_list = []
                
        for idx, child in enumerate(children):
            
            print("now crawling children: ", idx, " of project: ", pr_nr)
            
            comment_count = child.find_element_by_xpath('.//div/article/footer/div/div/span[1]').text
            like_count = child.find_element_by_xpath('.//div/article/footer/div/div/span[2]').text
            
            detail_link = child.get_attribute("href")
            
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
            vids = iframe.find_elements_by_tag_name("vid")
            video_count = len(vids)
            gif_counter = 0
            pic_counter = 0
            for pic in pics:
                if ".gif" in str(pic):
                    gif_counter +=1
                else:
                    pic_counter +=1
            
            sleep(5)
            
            new_dict = {'ID': update_id,
                        'Name': name,
                        'AuthorTitle': title_name,
                        'Time': time,
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
                
        
        my_item_func = lambda x: 'update'
        
        test_xml = dicttoxml(update_list, 
                             custom_root="updates", 
                             attr_type=False,
                             item_func=my_item_func)
        dom = parseString(test_xml)        
        
        #test_pretty_xml = dom.toprettyxml()                
        
        with open(path + "/" + "update_" + pr_nr + ".xml", mode="w", encoding="utf-8") as f:
                f.write(dom.toprettyxml())
        

    except Exception as e:
        print("Error -> Mark Link as Error in Database")
        print(e)
        return


    return faq


if __name__ == "__main__":
    
    metadata_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Python_Analysis/data/overview_files"
    overview_file = pd.read_csv(os.path.join(metadata_path, "update_metadata.csv"))
    
    for row in overview_file.itertuples(index=True, name="Project"):
        
        if row.updates_downloaded == False:  # only download projects that have not been downloaded before
            faq_count = crawl_updates(
                link=row.Link,
                pr_nr=row.Project_Nr,
                path=row.save_path)
            
            overview_file.at[row.Index, "updates_downloaded"] = True



#%% this is only here for archive reasons to know how to run the script individually without overview file
save_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/XML_local_download/art/Updates"

get_all_input(link="https://www.kickstarter.com/projects/1039804339/ben-newmans-matador",
              path = save_path,
              pr_nr = "1039804339")
