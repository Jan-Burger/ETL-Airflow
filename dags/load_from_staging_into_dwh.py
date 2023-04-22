import datetime as dt

from airflow import Dataset
from airflow.decorators import task, dag
from etl.webscraper import get_stock_summary, scrape_stock_details
from etl.transformer import transform_stock_data
from etl.db_loader import load_stock_data_into_staging

staging_area = Dataset('postgresql+psycopg2://localhost:5431/stocks_sentiment_staging')

@dag(schedule=None, start_date=dt.datetime(2023, 4, 21), catchup=False, tags=["stockdata", "apewisdom"])
def load_stock_data_into_staging_area():

    df_stock_summary = get_stock_summary()
    stocks_details = scrape_stock_details(df_stock_summary)
    transformed_stock_data = transform_stock_data(stock_data_summary=df_stock_summary, stock_details=stocks_details)
    load_stock_data_into_staging(transformed_stock_data)


dag = load_stock_data_into_staging_area()
