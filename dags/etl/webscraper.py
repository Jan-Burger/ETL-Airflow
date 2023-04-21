import re
import requests
import datetime as dt
import pandas as pd
from bs4 import BeautifulSoup
#from airflow.decorators import task, dag


#@task()
def get_stock_summary():
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

    return result_list


#@task()
def scrape_stock_details(stock_summary):

    df_summary = pd.DataFrame(stock_summary)

    result_list: list = list()

    for stock_ticker_symbol in df_summary["ticker"].to_list():

        print(stock_ticker_symbol)

        url: str = f"https://apewisdom.io/stocks/{stock_ticker_symbol}/"

        response = requests.get(url)

        soup = BeautifulSoup(response.text, 'html.parser')

        summary_divs = soup.find_all("div", {"class": "tile-value"})

        # if the mentions of the stock are 0 continue
        if int(re.findall(r'\d+', summary_divs[0].text)[0]) == 0:
            continue

        stock_details_js_string: str = soup.findAll('script')[5].string

        # Extract time and number of mentions from the js string
        times: list = re.findall("labels:.*],", stock_details_js_string)[0][9:-2].split(",")
        number_of_mentions: list = re.findall("data:.*],", stock_details_js_string)[0][7:-2].split(",")

        # Convert both lists datatypes into integers
        if times and number_of_mentions:
            times: list = [int(time[:-3]) for time in times]
            number_of_mentions: list = [int(mentions) for mentions in number_of_mentions]
        else:
            times = list()
            number_of_mentions = list()

        # Scrape sentiment score
        sentiment_score_div_text: str = summary_divs[-1].text
        sentiment_score: int = int(re.findall(r'\d+', sentiment_score_div_text)[0])

        # Scrape Keywords
        keywords_divs = soup.find_all("div", {"class": "row nearby-keywords"})

        keyword_spans = keywords_divs[0].find_all("span", {"class": "badge badge-filter"})

        keywords = dict()

        for keyword_span in keyword_spans:
            keyword, count = keyword_span.text.split(" x")

            keywords[keyword] = int(count)

        result_list.append({"ticker_symbol": stock_ticker_symbol,
                            "times": times,
                            "number_of_mentions": number_of_mentions,
                            "keywords": keywords,
                            "sentiment_score": sentiment_score})

    return result_list
