
import pandas as pd
import requests
import sqlite3
import json

#%%
"Part One - Extract Ghana Holiday Data from the Calendar API"

API_KEY = "f58dcaeb931c04722b51f051140d745d28b70a63"
COUNTRY = "gh"
YEAR = 2021
URL = f"https://calendarific.com/api/v2/holidays?api_key={API_KEY}&country={COUNTRY}&year={YEAR}"
r = requests.get(URL)
DATA = r.text
P = json.loads(DATA)
P2 = P['response'] 

holiday_names = []
type_holiday = []
date = []

for number in list(range(len(P2['holidays']))):
    holiday_names.append(P2['holidays'][number]['name'])
    type_holiday.append(P2['holidays'][number]['type'][0])
    date.append(P2['holidays'][number]['date']['iso'])

Ghana_holidays_dict = {"holiday_names": holiday_names, "type_holiday": type_holiday, "date": date}


Ghana_holidays = pd.DataFrame(Ghana_holidays_dict, columns= ["holiday_names", "type_holiday", "date"])

#%%
"Part Two - Tranformation stage of the Ghana Holiday Data from the Calendar API - Validation Stage"

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No Holidays downloaded. Finishing execution")
        return False
    
    # Primary Key Check
    if pd.Series(df["date"]).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")
        
    # Check for nulls 
    if df.isnull().values.any():
        raise Exception("Null values found")


if check_if_valid_data(Ghana_holidays):
    print("Data valid, proceed to Load stage")

#%%
"Part Three - Load stage of the Ghana Holiday Data from the Calendar API "
conn = sqlite3.connect("Holiday.db")
c = conn.cursor()


sql = "CREATE TABLE ghana_holidays ( "
sql +=" holiday_names text, type_holiday text, date text) "

c.execute(sql)

sql = "INSERT INTO ghana_holidays ( holiday_names, type_holiday, date) "
sql += "VALUES (?,?,?) "

ghana_data_list = list(Ghana_holidays.values.tolist())

for data in ghana_data_list:
    c.execute(sql, data)
    
    
conn.commit()
conn.close()
