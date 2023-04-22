from airflow.decorators import task
from dags.load_stockdata_into_staging import staging_area


@task(outlets=[staging_area])
def load_stock_data_into_staging(transformed_stock_data):

    import pandas as pd
    import datetime as dt
    from sqlalchemy import create_engine
    from sqlalchemy.types import JSON

    df_stock_summary = pd.read_json(transformed_stock_data["stock_summary_data"])
    df_stock_details = pd.read_json(transformed_stock_data["stock_detail_data"])

    df_stock_summary["date_pulled"] = df_stock_summary["date_pulled"].apply(dt.datetime.fromtimestamp)

    df_stock_details["date_pulled"] = df_stock_details["date_pulled"].apply(dt.datetime.fromtimestamp)
    df_stock_details["date_time_mentions"] = df_stock_details["date_time_mentions"].apply(dt.datetime.fromtimestamp)

    engine = create_engine('postgresql+psycopg2://postgres:travelblogger24.de@host.docker.internal:5431/stocks_sentiment_staging')

    df_stock_summary.to_sql(name="stocks_sentiment_agg_by_day", con=engine, if_exists="append", index=False,
                            dtype={"keywords": JSON})

    df_stock_details.to_sql(name="stocks_sentiment_details", con=engine, if_exists="append", index=False)



