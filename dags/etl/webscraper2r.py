import re
import requests
import datetime as dt
import pandas as pd
from bs4 import BeautifulSoup


def get_stock_summary() -> pd.DataFrame:
    page: int = 1

    result_list: list = list()

    while True:

        url: str = f"https://apewisdom.io/api/v1.0/filter/all-stocks/page/{page}"

        response = requests.get(url)

        for result in response.json()["results"]:
            result_list.append(result)

        page += 1

        if not response.json()["results"]:
            break

    return pd.DataFrame(result_list)


