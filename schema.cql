DROP KEYSPACE  counters;
CREATE KEYSPACE counters WITH
	replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE counters;

CREATE TABLE approx_distinct (
	row_key VARCHAR,
	column_key VARCHAR,
	shard_id VARCHAR,
	counter_state BLOB,
	PRIMARY KEY (row_key, column_key, shard_id)
);


CREATE TABLE sum (
	row_key VARCHAR,
	column_key VARCHAR,
	shard_id VARCHAR,
	counter_state BLOB,
	PRIMARY KEY (row_key, column_key, shard_id)
);


CREATE TABLE min (
	row_key VARCHAR,
	column_key VARCHAR,
	shard_id VARCHAR,
	counter_state BLOB,
	PRIMARY KEY (row_key, column_key, shard_id)
);

CREATE TABLE max (
	row_key VARCHAR,
	column_key VARCHAR,
	shard_id VARCHAR,
	counter_state BLOB,
	PRIMARY KEY (row_key, column_key, shard_id)
);