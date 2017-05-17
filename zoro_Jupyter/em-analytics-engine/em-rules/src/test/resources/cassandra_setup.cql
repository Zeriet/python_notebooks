CREATE KEYSPACE iepmaster WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE iepmaster;

CREATE TABLE iepmaster.events_hourly (event_bucket text,yyyymmdd text,hour int,log_uuid timeuuid,measures_aggr map<text, double>,measures_avg map<text, double>,measures_cnt map<text, double>,measures_max map<text, double>,measures_min map<text, double>,tags map<text, text>,PRIMARY KEY (event_bucket, yyyymmdd, hour)) WITH CLUSTERING ORDER BY (yyyymmdd DESC, hour ASC);

