with ranked_view as (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY detailed_timestamp, device ORDER BY humidity) AS row_num, 
    datediff(received, timestamp) as delay
    FROM TEMP

)

select 
    device,
    CO2_level,
    humidity,
    temperature,
    timestamp,
    detailed_timestamp,
    received

    from ranked_view
    where row_num = 1 and delay < 2