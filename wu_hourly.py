#import numpy as np
import pandas as pd
#import pyarrow as pa
#import pyarrow.parquet as pq
#from datetime import datetime
from datetime import datetime, date#, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService

# code adapted from the following sources:
# https://medium.com/swlh/web-scraping-weather-data-with-selenium-webdriver-69a7b501fac
# https://stackoverflow.com/questions/71776919/scraping-wind-data-from-weather-underground-using-selenium-in-python

# define future date
start_date = date.today() + pd.Timedelta(days=1)

# get data for Portland. KORPORTL554 is Train Vault Station for consistency (may be removed).
# https://www.wunderground.com/hourly/us/or/portland/KORPORTL554/date/2023-03-18
page = 'https://www.wunderground.com/hourly/us/or/portland/KORPORTL554/date/{}-{}-{}'

df = pd.DataFrame()

options = webdriver.ChromeOptions()
#options.add_argument("--disable-gpu")
#options.add_argument("--disable-extensions")
#options.add_argument("--disable-infobars")
#options.add_argument("--start-maximized")
#options.add_argument("--disable-notifications")
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
#options.set_capability("loggingPrefs", {'performance': 'ALL'})
chrome_prefs = {}
options.experimental_options["prefs"] = chrome_prefs
chrome_prefs["profile.default_content_settings"] = {"images": 2}
#service = ChromeService(executable_path='chromedriver.exe')
service = ChromeService(executable_path='/usr/local/bin/chromedriver')
driver = webdriver.Chrome(service=service, options=options)

classlist = ["mat-cell cdk-cell cdk-column-timeHour mat-column-timeHour ng-star-inserted",
             "mat-cell cdk-cell cdk-column-conditions mat-column-conditions ng-star-inserted",
             "mat-cell cdk-cell cdk-column-temperature mat-column-temperature ng-star-inserted",
             "mat-cell cdk-cell cdk-column-feelsLike mat-column-feelsLike ng-star-inserted",
             "mat-cell cdk-cell cdk-column-precipitation mat-column-precipitation ng-star-inserted",
             "mat-cell cdk-cell cdk-column-liquidPrecipitation mat-column-liquidPrecipitation ng-star-inserted",
             "mat-cell cdk-cell cdk-column-cloudCover mat-column-cloudCover ng-star-inserted",
             "mat-cell cdk-cell cdk-column-dewPoint mat-column-dewPoint ng-star-inserted",
             "mat-cell cdk-cell cdk-column-humidity mat-column-humidity ng-star-inserted",
             "mat-cell cdk-cell cdk-column-wind mat-column-wind ng-star-inserted",
             "mat-cell cdk-cell cdk-column-wind mat-column-wind ng-star-inserted",
             "mat-cell cdk-cell cdk-column-pressure mat-column-pressure ng-star-inserted"]

name = ['time', 'conditions', 'temp_f', 'feelslike_f', 'precipchance', 'precipamount_in', 'cloudcover', 'dewpoint_f', 'humidity', 'windspeed_mph', 'winddirection', 'pressure_in']

print('gathering data from: ', start_date)
    
formatted_lookup_URL = page.format(start_date.year,
                                   start_date.month,
                                   start_date.day)

print(formatted_lookup_URL)
driver.get(formatted_lookup_URL)

rows = WebDriverWait(driver, 20).until( \
    EC.visibility_of_all_elements_located((By.XPATH, \
    '//td[@class="' + classlist[0] + '"]')))

for row in rows:

    time = row.find_element(By.XPATH,'.//span[@class="ng-star-inserted"]').text
    
    # append new row to table
    #df = df.append(pd.DataFrame({"Day":[str(start_date)],"time":[time],}),
    #               ignore_index = True)
    df = pd.concat([df, pd.DataFrame({"Day":[str(start_date)],"time":[time],})],
                   ignore_index = True)

del classlist[0]
del name[0]

for ii in range(len(classlist)):
    
    rows = WebDriverWait(driver, 20).until( \
        EC.visibility_of_all_elements_located((By.XPATH, \
        '//td[@class="' + classlist[ii] + '"]')))
    
    for row in rows:

        if name[ii]=='winddirection':
            data = row.find_element(By.XPATH,
                './/span[@class="wu-suffix ng-star-inserted"]').text
            #print(data)

        elif name[ii]=='conditions':
            data = row.find_element(By.XPATH,
                './/span[@class="show-for-medium conditions"]').text
            #print(data)

        else:
            data = row.find_element(By.XPATH,
                './/span[@class="wu-value wu-value-to"]').text
        
        # append new row to table
        #df = df.append(pd.DataFrame({name[ii]:[data]}), ignore_index=True)
        df = pd.concat([df, pd.DataFrame({name[ii]:[data]})], ignore_index=True)
    
driver.quit()

# remove NaN
df = df.apply(lambda x: pd.Series(x.dropna().values))
#print(df.head())

# write to parquet
filename = '{}-{}-{}_portland_forecast_{}.parquet.gzip'.format(start_date.year, start_date.month, start_date.day, str(datetime.now().time()).replace(':', '.'))
df.to_parquet('/app/output/{}'.format(filename))

# this doesn't seem like it's going to work with just standard python dockerfile.... check out docker selenium or selenoid