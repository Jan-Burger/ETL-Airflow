import datetime as dt

from airflow import Dataset
from airflow.decorators import task, dag
from etl.dwh_loader import extract_data_from_staging_area

staging_area = Dataset('postgresql+psycopg2://localhost:5431/stocks_sentiment_staging')


@dag(schedule=[staging_area], start_date=dt.datetime(2023, 4, 22), catchup=False, tags=["stockdata", "apewisdom"])
def load_from_staging_into_dwh():
    extract_data_from_staging_area()


dag = load_from_staging_into_dwh()
