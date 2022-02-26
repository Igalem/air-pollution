truncate table air_pollution_mrr;

insert into air_pollution_mrr
select 
    date(substring(ts,1,10)) as ts,
    city,
    case when lower(value) = 'aqius' then value
        else measure end measure,
    case when lower(value) = 'aqius' then round(measure,2)
        else round(value,2) end value,
    color,
    label
from air_pollution_api
;