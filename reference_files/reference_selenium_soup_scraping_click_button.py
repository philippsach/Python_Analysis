from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException,StaleElementReferenceException
from bs4 import BeautifulSoup
import re
driver = webdriver.Chrome(executable_path="/Users/philippsach/.conda/envs/Python_Analysis/bin/chromedriver")
driver.get("https://www.khanacademy.org/computing/computer-science/algorithms/intro-to-algorithms/v/what-are-algorithms")

while True:
    try:
        showmore=WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH ,'//*[@id="v/what-are-algorithms-panel"]/div[1]/div/div[6]/div/div[4]/button')))
        showmore.click()
    except TimeoutException:
        break
    except StaleElementReferenceException:
        break


soup=BeautifulSoup(driver.page_source,'html.parser')
#find the profile links
profiles = soup.find_all(href=re.compile("/profile/kaid"))
profile_list=[]
for links in profiles:
    links_no_list = links.extract()
    text_link = links_no_list['href']
    text_link_nodiscussion = text_link[:-10]
    final_profile_link ='https://www.khanacademy.org'+text_link_nodiscussion
    profile_list.append(final_profile_link)

#remove duplicates
#remove the below line if you want the dupliactes
profile_list=list(set(profile_list))

#print number of profiles we got
print(len(profile_list))
#create the csv file
filename = "khanscraptry1.csv"
f = open(filename, "w")
headers = "link, date_joined, points, videos, questions, votes, answers, flags, project_request, project_replies, comments, tips_thx, last_date\n"
f.write(headers)


#for each profile link, scrape the specific data and store them into the csv
for link in profile_list:
    #to avoid Scrapping same profile multiple times
    #print each profile link we are about to scrap
    print("Scrapping ",link)
    driver.get(link)
    #wait for content to load
    #if profile does not exist skip
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH ,'//*[@id="widget-list"]/div[1]/div[1]')))
    except TimeoutException:
        continue
    soup=BeautifulSoup(driver.page_source,'html.parser')
    user_info_table=soup.find('table', class_='user-statistics-table')
    if user_info_table is not None:
        dates,points,videos=[tr.find_all('td')[1].text for tr in user_info_table.find_all('tr')]
    else:
        dates=points=videos='NA'

    user_socio_table=soup.find_all('div', class_='discussion-stat')
    data = {}
    for gettext in user_socio_table:
        category = gettext.find('span')
        category_text = category.text.strip()
        number = category.previousSibling.strip()
        data[category_text] = number

    full_data_keys=['questions','votes','answers','flags raised','project help requests','project help replies','comments','tips and thanks'] #might change answers to answer because when it's 1 it's putting NA instead
    for header_value in full_data_keys:
        if header_value not in data.keys():
            data[header_value]='NA'

    user_calendar = soup.find('div',class_='streak-calendar-scroll-container')
    if user_calendar is not None:
        last_activity = user_calendar.find('span',class_='streak-cell filled')
        try:
            last_activity_date = last_activity['title']
        except TypeError:
            last_activity_date='NA'
    else:
        last_activity_date='NA'
    f.write(link + "," + dates + "," + points.replace("," , "") + "," + videos + "," + data['questions'] + "," + data['votes'] + "," + data['answers'] + "," + data['flags raised'] + "," + data['project help requests'] + "," + data['project help replies'] + "," + data['comments'] + "," + data['tips and thanks'] + "," + last_activity_date + "\n")