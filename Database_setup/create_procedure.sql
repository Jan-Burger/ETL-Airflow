create or replace procedure test_procedure()
language plpgsql
as $$
declare

begin
	DELETE FROM stocks_sentiment_agg_by_day
	WHERE date_pulled < DATE((now() - interval '30 day'))::TIMESTAMP;
	
	DELETE FROM stocks_sentiment_details
	WHERE date_pulled < DATE((now() - interval '30 day'))::TIMESTAMP;
end; $$