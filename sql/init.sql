CREATE TABLE IF NOT EXISTS air_pollution_api
( 
    ts varchar(255),
    city varchar(255),
    measure varchar(255),
    value varchar(255),
    color varchar(255),
    label varchar(255)
);

CREATE TABLE IF NOT EXISTS air_pollution_mrr
( 
    ts date,
    city varchar(255),
    measure varchar(255),
    value float,
    color varchar(255),
    label varchar(255)
);