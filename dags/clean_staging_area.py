import datetime as dt

from airflow import Dataset
from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

dwh = Dataset('postgresql+psycopg2://localhost:5431/stocks_sentiment_dwh')


@dag(schedule=[dwh], start_date=dt.datetime(2023, 4, 22), catchup=False, tags=["stockdata", "apewisdom"])
def clean_staging_area():

    clean_staging_area = PostgresOperator(
        task_id="clean_staging_area",
        postgres_conn_id="postgres_host_machine",
        sql="CALL clean_staging_area()"
    )


dag = clean_staging_area()
