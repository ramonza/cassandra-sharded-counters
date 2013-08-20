DROP KEYSPACE  counters;
CREATE KEYSPACE counters WITH
	replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE counters;

CREATE TABLE approx_distinct (
	day VARCHAR,
	hour INT,
	shard_id VARCHAR,
	counter_state BLOB,
	PRIMARY KEY (day, hour, shard_id)
);


CREATE TABLE sum (
	day VARCHAR,
	hour INT,
	shard_id VARCHAR,
	counter_state BLOB,
	PRIMARY KEY (day, hour, shard_id)
);


CREATE TABLE min (
	day VARCHAR,
	hour INT,
	shard_id VARCHAR,
	counter_state BLOB,
	PRIMARY KEY (day, hour, shard_id)
);

CREATE TABLE max (
	day VARCHAR,
	hour INT,
	shard_id VARCHAR,
	counter_state BLOB,
	PRIMARY KEY (day, hour, shard_id)
);