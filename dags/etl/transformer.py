import pandas as pd
from airflow.decorators import task

@task()
def transform_stock_data(stock_data_summary, stock_details):
    print("DATA SUCCESFULLY ARRIVED IN TRANSFORM FUNCTION !!!!! NICE !!!!!!")