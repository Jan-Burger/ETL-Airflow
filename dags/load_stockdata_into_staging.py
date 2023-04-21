import datetime as dt

import pandas as pd
from airflow.decorators import task, dag
from etl.webscraper import get_stock_summary, scrape_stock_details
from etl.transformer import transform_stock_data


@dag(schedule=None, start_date=dt.datetime(2023, 4, 21), catchup=False, tags=["stockdata", "apewisdom"])
def load_stock_data_into_staging_area():

    df_stock_summary = get_stock_summary()
    stocks_details = scrape_stock_details(df_stock_summary)
    transform_stock_data(stock_data_summary=df_stock_summary, stock_details=stocks_details)


dag = load_stock_data_into_staging_area()
