from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from bs4 import BeautifulSoup
import re

driver = webdriver.Chrome(executable_path="/Users/philippsach/.conda/envs/Python_Analysis/bin/chromedriver")
driver.get("https://www.kickstarter.com/projects/arten2/the-art-of-eddie-nunez/comments")


# click the button to see more
while True:
    try:
        showmore = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
            (By.XPATH, '//*[@id="react-project-comments"]/div/button')))
        showmore.click()



    except TimeoutException:
        break
    except StaleElementReferenceException:
        break