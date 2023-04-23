BEGIN;


CREATE TABLE IF NOT EXISTS public.stock
(
    ticker_symbol character varying(10) NOT NULL,
    name text,
    PRIMARY KEY (ticker_symbol)
);

CREATE TABLE IF NOT EXISTS public.keyword
(
    id serial NOT NULL,
    ticker_symbol character varying(10) NOT NULL,
    keyword text NOT NULL,
    frequency integer,
    date_time timestamp without time zone NOT NULL,
    CONSTRAINT test PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.stock_mentions_by_day
(
    id serial NOT NULL,
    ticker_symbol character varying(10) NOT NULL,
    date_time timestamp without time zone NOT NULL,
    mentions integer,
    upvotes integer,
    sentiment_score integer,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.stock_mentions_by_minute
(
    id serial NOT NULL,
    ticker_symbol character varying(10) NOT NULL,
    date_time timestamp without time zone NOT NULL,
    mentions integer,
    PRIMARY KEY (id)
);

ALTER TABLE IF EXISTS public.keyword
    ADD CONSTRAINT ticker_symbol_foreign_key FOREIGN KEY (ticker_symbol)
    REFERENCES public.stock (ticker_symbol) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.stock_mentions_by_day
    ADD CONSTRAINT ticker_symbol_foreign_key FOREIGN KEY (ticker_symbol)
    REFERENCES public.stock (ticker_symbol) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.stock_mentions_by_minute
    ADD CONSTRAINT ticker_symbol_foreign_key FOREIGN KEY (ticker_symbol)
    REFERENCES public.stock (ticker_symbol) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;