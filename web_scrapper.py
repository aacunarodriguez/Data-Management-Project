import undetected_chromedriver as uc
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import pandas as pd
from selenium.common.exceptions import TimeoutException
import sys
import mysql.connector
import traceback


def get_web_scrapper_data():
    print("Running web_scrapper.py")
    print("Extracting data from https://www.zillow.com and sending to DB")

    try:
        db = mysql.connector.connect(
            host="127.0.0.1",
            port="3306",
            user="root",
            password="oiecy321",
        )
        
        cursor = db.cursor()
        
        options = Options()
        driver = uc.Chrome(options=options)
        driver.set_window_size(1400, 800)
        
        cursor.execute("CREATE DATABASE IF NOT EXISTS web_scrapper_api_property_data")
        cursor.execute("USE web_scrapper_api_property_data")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS web_scrapper_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                id_location VARCHAR(255),
                price INT,
                state VARCHAR(255),
                bedrooms INT,
                bathrooms INT,
                sq_ft INT
            )
        """)
        
        xpath_property = '//div[contains(@class, "property-card-data")]'
        xpath_pagination_bar = '//nav[@aria-label="Pagination"]'

        # Pages to scrape in https://www.zillow.com
        for i in range(1, 20):
            print(f"Analyzing page {i}")
            
            if i == 1:
                URL = f"https://www.zillow.com/us/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-122.63179707246022%2C%22east%22%3A-71.56734394746022%2C%22south%22%3A22.76716078081583%2C%22north%22%3A50.32519080304388%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A102001%2C%22regionType%22%3A1%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%2C%22land%22%3A%7B%22value%22%3Afalse%7D%2C%22price%22%3A%7B%22min%22%3A50000%7D%2C%22mp%22%3A%7B%22min%22%3A250%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A5%7D"
            else:
                URL = f"https://www.zillow.com/us/{i}_p/?searchQueryState=%7B%22pagination%22%3A%7B%22currentPage%22%3A{i}%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-125.09273457246022%2C%22east%22%3A-69.10640644746022%2C%22south%22%3A15.528949991642342%2C%22north%22%3A54.97199868502722%7D%2C%22mapZoom%22%3A4%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A102001%2C%22regionType%22%3A1%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22price%22%3A%7B%22min%22%3A50000%2C%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A250%2C%22max%22%3Anull%7D%2C%22land%22%3A%7B%22value%22%3Afalse%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%7D%2C%22isListVisible%22%3Atrue%7D"
            
            driver.get(URL)
            time.sleep(2)
            try:
                pagination_bar = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, xpath_pagination_bar))
                )
            except TimeoutException:
                driver.refresh()
                time.sleep(5)
                try:
                    pagination_bar = WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.XPATH, xpath_pagination_bar))
                        )
                except TimeoutException:
                    print("There is a problem with the page. Its not loading properly. Exiting script.")
                    sys.exit()
                        
            driver.execute_script("arguments[0].scrollIntoView({ behavior: 'smooth' });", pagination_bar)
            time.sleep(1)
            
            properties_elements = driver.find_elements(By.XPATH, xpath_property)
            
            for property_info in properties_elements:
                property_text = property_info.text
                id_location = " ".join(property_text.split('\n')[:2])
                
                cursor.execute("SELECT EXISTS(SELECT * FROM web_scrapper_table WHERE id_location = %s)", (id_location,))
                find_id_location_in_db = cursor.fetchone()[0]
                
                if find_id_location_in_db == 0:
            
                    price_match = re.search(r"\$\d{1,3}(?:,\d{3})*", property_text)
                    price = float(price_match.group(0).replace("$", "").replace(",", "")) if price_match else None
                    
                    state_match = re.search(r",\s([A-Z]{2})\s\d{5}", property_text)
                    state = state_match.group(1) if state_match else None
                    
                    bedrooms_match = re.search(r"(\d+)\sbd[s]?", property_text)
                    bedrooms = int(bedrooms_match.group(1).strip()) if bedrooms_match else None
                    
                    bathrooms_match = re.search(r"(\d+)\sba[s]?", property_text)
                    bathrooms = int(bathrooms_match.group(1).strip()) if bathrooms_match else None
                    
                    sq_ft_match = re.search(r"(\d{1,3}(?:,\d{3})*)\ssqft", property_text)
                    sq_ft = int(sq_ft_match.group(1).replace(",", "").strip()) if sq_ft_match else None
                    
                    sql = "INSERT INTO web_scrapper_table (id_location, price, state, bedrooms, bathrooms, sq_ft) VALUES (%s, %s, %s, %s, %s, %s)"
                    val = (id_location, price, state, bedrooms, bathrooms, sq_ft)
                    cursor.execute(sql, val)
                    db.commit()
                
            print("Done.")
            time.sleep(2)

        time.sleep(2)
        driver.close()
        time.sleep(2)
        driver.quit()
        print("\n")
            
    except Exception as exp:
        print("Error in Web Scrapper. Exiting script")
        print(traceback.format_exc())
        print("\n")
        sys.exit()
        
    finally:
        cursor.close()
        db.close()
        driver.quit()
        print("Ending scrapper operation.")
        

    