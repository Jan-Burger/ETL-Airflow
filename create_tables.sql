BEGIN;


CREATE TABLE IF NOT EXISTS public.stocks_sentiment_agg_by_day
(
    id serial NOT NULL,
    date_pulled timestamp without time zone NOT NULL,
    rank integer,
    ticker_symbol character varying(10) NOT NULL,
    name text,
    mentions integer NOT NULL,
    upvotes integer NOT NULL,
    rank_24h_ago integer,
    mentions_24h_ago integer NOT NULL,
    sentiment_score integer,
    keywords json[],
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.stocks_sentiment_details
(
    id serial NOT NULL,
    date_pulled timestamp without time zone NOT NULL,
    date_time_mentions timestamp without time zone NOT NULL,
    mentions integer NOT NULL,
    PRIMARY KEY (id)
);
END;