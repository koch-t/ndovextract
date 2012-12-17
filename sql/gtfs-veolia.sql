COPY (
SELECT
'VTN' as agency_id,
'Veolia' as agency_name,
'http://www.veolia.nl/' as agency_url,
'Europe/Amsterdam' as agency_timezone,
'nl' as agency_lang
) TO '/tmp/agency.txt' WITH CSV HEADER;
