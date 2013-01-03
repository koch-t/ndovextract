COPY (
SELECT
'RET' as agency_id,
'RET' as agency_name,
'http://www.ret.nl/' as agency_url,
'Europe/Amsterdam' as agency_timezone,
'nl' as agency_lang
) TO '/tmp/agency.txt' WITH CSV HEADER;
