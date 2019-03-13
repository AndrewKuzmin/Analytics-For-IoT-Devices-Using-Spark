DROP KEYSPACE IF EXISTS nest_db;
CREATE KEYSPACE nest_db WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use nest_db;

CREATE TABLE  nest_thermostat (
    version text,
    device_id text,
    locale text,
    location text,
    last_connected text,
    humidity double,
    target_temperature_f double,
    mode text,
    name text,
    id double,
    PRIMARY KEY(id)  
);
