Complex counters example with Cassandra, JRuby and stream-lib
===

Cassandra has counter columns but they only support simple increment/decrement of integer values. There are also
[some issues](http://wiki.apache.org/cassandra/Counters#Technical_limitations) with these. This is my attempt at a scheme
for more robust and general counters on top of standard (non-counter) Cassandra columns mostly to help me
learn Cassandra.

This implementation satisfies the following constraints:

* Supports arbitrary [CRDT](http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf)s
* Counters can be read/updated on as many nodes as we like
* Updates never read from Cassandra
* All writes to Cassandra are idempotent
* Mutator memory can be limited without affecting updates

The basic approach is to shard counters by some unique ID "owned" by a particular mutator. Then, we add some structure
to these IDs that let us know when a particular shard will never be updated again. Readers locate all shards associated
with a counter and merge them, optionally garbage collecting any finalized shards.

One of the limitations of this approach is that reads slow linearly with the number of mutators, although I would guess
that constant factors would dominate up to a few dozen mutators.

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
    $ bin/rake create_schema start_servers
    # patiently wait for 10 Puma instances to start up (output like "Puma 2.5.1 starting...")
    $ bin/rake test multiprocess_test

License
---

Public domain, not fit for any particular purpose.



