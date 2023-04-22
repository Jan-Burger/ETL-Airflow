from airflow.decorators import task


@task(multiple_outputs=True)
def transform_stock_data(stock_data_summary, stock_details):
    import pandas as pd
    import datetime as dt

    """
    Transform stock summary data
    """
    date_pulled = str(pd.Timestamp.now().round("S").timestamp())[:-2]

    df_stock_summary = pd.DataFrame(stock_data_summary)

    df_stock_summary.loc[df_stock_summary["rank_24h_ago"] == "0", "rank_24h_ago"] = None
    df_stock_summary.loc[df_stock_summary["mentions_24h_ago"].isnull(), "mentions_24h_ago"] = "0"
    df_stock_summary.loc[df_stock_summary["mentions"].isnull(), "mentions"] = "0"
    df_stock_summary.loc[df_stock_summary["upvotes"].isnull(), "upvotes"] = "0"

    df_stock_summary = df_stock_summary.astype({"rank": "int32",
                                                "ticker": "object",
                                                "name": "object",
                                                "mentions": "int32",
                                                "upvotes": "int32",
                                                "rank_24h_ago": "object",
                                                "mentions_24h_ago": "int32"})

    def match_stock_sentiment_score(ticker_symbol):
        for detail in stock_details:
            if detail["ticker_symbol"] == ticker_symbol:
                return detail["sentiment_score"]

        return None

    def match_stock_keywords(ticker_symbol):
        for detail in stock_details:
            if detail["ticker_symbol"] == ticker_symbol:
                return detail["keywords"]

        return None

    df_stock_summary["sentiment_score"] = df_stock_summary["ticker"].apply(match_stock_sentiment_score)

    df_stock_summary["date_pulled"] = date_pulled

    df_stock_summary["keywords"] = df_stock_summary["ticker"].apply(match_stock_keywords)

    df_stock_summary.rename(columns={"ticker": "ticker_symbol"}, inplace=True)

    """
    Transform stock detail data
    """
    df_stock_details = pd.DataFrame(stock_details)

    df_stock_details.drop(columns=["keywords", "sentiment_score"], inplace=True)

    df_stock_details = df_stock_details.explode(["times", "number_of_mentions"]).reset_index()

    df_stock_details["date_pulled"] = date_pulled

    df_stock_details.rename(columns={"times": "date_time_mentions", "number_of_mentions": "mentions"}, inplace=True)

    df_stock_details.drop(columns=["index"], inplace=True)

    df_stock_details["date_time_mentions"] = df_stock_details["date_time_mentions"].astype(str)

    return {"stock_summary_data": df_stock_summary.to_json(), "stock_detail_data": df_stock_details.to_json()}

