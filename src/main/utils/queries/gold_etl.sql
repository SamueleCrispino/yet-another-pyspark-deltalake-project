select 

device,
humidity,
CO2_level,
temperature, 
timestamp,
month(timestamp) as month,
detailed_timestamp,
type,
area,
customer,
received

from TEMP
left join delta.`/bronze/devices` as devices
on TEMP.device = devices.code