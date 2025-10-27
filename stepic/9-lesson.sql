CREATE OR REPLACE FUNCTION current_time_utc()
RETURNS timestamptz
AS $$
	SELECT now()
$$ LANGUAGE SQL;

SELECT current_time_utc()


