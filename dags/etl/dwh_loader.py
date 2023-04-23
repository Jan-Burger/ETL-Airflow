from airflow.decorators import task

@task()
def extract_data_from_staging_area():

    import pandas as pd
    from sqlalchemy import create_engine

    # Connection to DWH
    engine_staging_area = create_engine(
        'postgresql+psycopg2://postgres:travelblogger24.de@host.docker.internal:5431/stocks_sentiment_staging')
    engine_dwh = create_engine(
        'postgresql+psycopg2://postgres:travelblogger24.de@host.docker.internal:5431/stocks_sentiment_dwh')

    # Get Info about current DWH Data situation
    max_date_stock_mentions_by_day_dwh = pd.read_sql("SELECT max(date_time) as max_date_time FROM stock_mentions_by_day", engine_dwh).iloc[0, 0]
    max_date_stock_mentions_by_minute_dwh = pd.read_sql("SELECT max(date_time) as max_date_time FROM stock_mentions_by_minute", engine_dwh).iloc[0, 0]

    stocks_dwh = pd.read_sql("SELECT ticker_symbol from stock", engine_dwh)

    # Get data from staging area
    if max_date_stock_mentions_by_day_dwh:
        df_stock_mentions_by_day = pd.read_sql(
            f"SELECT * FROM stocks_sentiment_agg_by_day WHERE date_pulled > '{max_date_stock_mentions_by_day_dwh.strftime('%Y-%m-%d %X')}'::TIMESTAMP",
            engine_staging_area)
    else:
        df_stock_mentions_by_day = pd.read_sql("SELECT * FROM stocks_sentiment_agg_by_day", engine_staging_area)

    if max_date_stock_mentions_by_minute_dwh:
        df_stock_mentions_by_minute = pd.read_sql(
            f"SELECT * FROM stocks_sentiment_details WHERE date_time_mentions > '{max_date_stock_mentions_by_minute_dwh.strftime('%Y-%m-%d %X')}'::TIMESTAMP",
            engine_staging_area)
    else:
        df_stock_mentions_by_minute = pd.read_sql("SELECT * FROM stocks_sentiment_details", engine_staging_area)

    def check_if_stock_in_dwh(ticker_symbol):
        if not ticker_symbol in stocks_dwh["ticker_symbol"].to_list():
            return True
        return False

    df_stock_mentions_by_day["to_upload_in_stocks"] = df_stock_mentions_by_day["ticker_symbol"].apply(check_if_stock_in_dwh)

    df_stocks_to_upload = df_stock_mentions_by_day.loc[df_stock_mentions_by_day["to_upload_in_stocks"] == True][["ticker_symbol", "name"]]

    df_keywords_to_upload = df_stock_mentions_by_day.loc[df_stock_mentions_by_day["keywords"].notnull() & df_stock_mentions_by_day["keywords"]][["ticker_symbol", "date_pulled", "keywords"]]

    df_keywords_to_upload["keys"] = df_keywords_to_upload["keywords"].apply(lambda x: [x for x in x.keys()])

    df_keywords_to_upload["values"] = df_keywords_to_upload["keywords"].apply(lambda x: [x for x in x.values()])

    df_keywords_to_upload = df_keywords_to_upload.explode(["keys", "values"])

    df_keywords_to_upload.drop(columns=["keywords"], inplace=True)
    df_keywords_to_upload.rename(columns={"date_pulled": "date_time", "keys": "keyword", "values": "frequency"},
                                 inplace=True)

    df_stock_mentions_by_day_to_upload = df_stock_mentions_by_day.drop(
        columns=["id", "rank", "name", "mentions_24h_ago", "rank_24h_ago", "keywords", "to_upload_in_stocks"])

    df_stock_mentions_by_day_to_upload.rename(columns={"date_pulled": "date_time"}, inplace=True)

    df_stock_mentions_by_minute.drop(columns=["id", "date_pulled"], inplace=True)

    df_stock_mentions_by_minute.rename(columns={"date_time_mentions": "date_time"}, inplace=True)

    # Load transformed Data into DWH
    loader_engine = create_engine(
        'postgresql+psycopg2://postgres:travelblogger24.de@host.docker.internal:5431/stocks_sentiment_dwh')

    df_stocks_to_upload.to_sql(name="stock", con=loader_engine, if_exists="append", index=False)

    df_stock_mentions_by_day_to_upload.to_sql(name="stock_mentions_by_day", con=loader_engine, if_exists="append",
                                              index=False)

    df_keywords_to_upload.to_sql(name="keyword", con=loader_engine, if_exists="append", index=False)

    df_stock_mentions_by_minute.to_sql(name="stock_mentions_by_minute", con=loader_engine, if_exists="append",
                                       index=False)
