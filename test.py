import pandas as pd
import requests
import datetime as dt


url = "https://apewisdom.io/api/v1.0/filter/all-stocks/page/56"

response = requests.get(url)

#print(response.json())

df = pd.DataFrame(response.json()["results"])

print(df.head())