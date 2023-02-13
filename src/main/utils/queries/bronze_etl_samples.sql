select 
CO2_level, 
device, 
humidity, 
temperature,
to_date(timestamp) as timestamp, 
to_timestamp(regexp_replace(regexp_replace(timestamp, 'T', ' '), 'Z', ''), 'yyyy-MM-dd HH:mm:ss.SSS') as detailed_timestamp,
received
from TEMP