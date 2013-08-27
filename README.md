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
    $ bin/rake create_schema
    $ bin/rake start_servers
    # patiently wait for 10 Puma instances to start up (output like "Puma 2.5.1 starting...")
    $ bin/rake test integration

License
---

This example code is in the public domain.



