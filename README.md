Sharded counters example with Cassandra, JRuby and stream-lib
===

Requirements
---

* JRuby 1.7.4
* [stream-lib](https://github.com/clearspring/stream-lib)
* Apache Cassandra

Running
---

You first need to install stream-lib:

	$ git clone git@github.com:clearspring/stream-lib.git && cd stream-lib
	$ mvn install

Then you need Cassandra running on localhost on the default port.

To run the tests:

    $ bundle install
    $ cqlsh < schema.cql
    $ bin/rake start_servers
    # patiently wait for 10 Puma instances to start up (output like "Puma 2.5.1 starting...")
    $ bin/rake test integration

Insert some values:

    $ bin/rackup -p 8000 -s puma &
    $ curl -X POST http://localhost:8000/approx_distinct/add/1
    $ curl http://localhost:8000/approx_distinct/hourly-summary

License
---

This example code is in the public domain.


TODO
---

"Garbage collect" the shards by periodically merging into a single counter. The way to safely do this
is to have each node increment it's shard id every day (say), then you know the previous days' values can
safely be assumed to be final.

