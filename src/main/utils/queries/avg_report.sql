select 

AVG(CO2_level) as avg_CO2_level, 
AVG(humidity) as avg_humidity, 
AVG(temperature) as avg_temperature

from TEMP

group by month, area
