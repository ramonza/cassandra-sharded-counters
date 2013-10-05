Order-free, sharded counters on Cassandra
===

Cassandra has counter columns but they only support simple increment/decrement of integer values. There are also
[some issues](http://wiki.apache.org/cassandra/Counters#Technical_limitations) with these. This is my attempt at a scheme
for more robust and general counters on top of standard (non-counter) Cassandra columns mostly to help me
learn Cassandra.

How it works
------------

Each counter is a tuple `<mutator_id, state, expires_at>` where `mutator_id` is a unique identifier for an actor
processing updates to the counter , `state` is the local state of mutator with id `mutator_id` and `expires_at` is a
timestamp that tells us when the mutator will die, used by the garbage collection process. A mutator never updates
it's shard after `expires_at` has passed. For example, in a sum counter the state would be just the local sum of
values seen by the indicated mutator.

When a mutator is born, it sets it's `expires_at` 24 hours in the future. For each update command it receives,
it check's that it's `expires_at` is at least an hour in the future. If it is, it applies the update locally and
serializes it's state to it's shard in Cassandra. Otherwise, it refuses the update and dies immediately. The client must
create a new mutator to apply this update.

To read the current total, any process can read all shards and add up the states locally (merge). A mutator can die
at any point without coordination, there is no guarantee that it lives until `expires_at`. Under memory pressure for
example, a mutator can be unceremoniously culled. If any client sends a subsequent update,
a new mutator will be created.

The problem now is that over time we accumulate shards. This requires a garbage collection (GC) process. The job of
garbage collection will be to clear out shards from dead mutators and merge their states into a single special shard
which we will call the tally.

Suppose that we read all shards (including any previous tally) at consistency level `ALL` and we find that some of
them have `expires_at` more than 24 hours in the past. Call this point in time 24 hours ago the GC cutoff. Under the
assumption that clocks are synchronized to within some very generous margin (say a few hours),
we can be certain that shards with `expires_at` older than the GC cutoff are _final_ (will never be updated): the
behaviour of the mutators guarantees no new updates will be generated, we are outside the window for hinted handoffs
to arrive asynchronously (`max_hint_window_in_ms`) and a successful read at CL `ALL` means we definitely got
everything.

Their state is added to the previous tally (or a blank one if there wasn't a previous tally) and we issue a Cassandra
 batch mutation deleting the shards and updating the tally with the new value that includes the state from all the
 deleted shards.

This is the only operation that updates values in Cassandra based on previously read values so we need to take
special care that it's safe in the presence of multiple concurrent, uncoordinated GC processes.

To this end, we would like to make the GC batch idempotent. That is, we must be able to read all the final shards as
well as the previous tally, compute the batch operation and then repeatedly send this operation to Cassandra once
every second for the next year (I find hyperbole a useful tool in appreciating Cassandra's LWW eventual consistency!)

We can do this if we set the timestamp on the batch operation higher than the highest observed `expires_at`. Then
repeatedly submitting our GC batch never overwrites a tally value that was created from newer shards (shards with
greater `expires_at`).

If some other GC process reads exactly the same expired shards as we do it will produce the same batch update so
there will be no conflict. If it reads a few more expired shards before our update has been applied,
the additional shards must all have `expires_at` greater than any shards that we considered (we have already
established that we have read the final and complete set of shards with `expires_at` less than the GC cutoff).  In this
 case the other process's update will have a greater timestamp than ours and so will overwrite our update. If the
 other process reads the shards after our update has been applied then it will also have observed our updated tally
 state and any new shards it considers will have `expires_at` greater than any that we considered.

The GC process requires all replicas to be up due to reads at CL `ALL` but since GC is just an optimization and
doesn't affect the correctness of the system, it can be deferred to times when the network is healthy. This
implementation attempts a garbage collection after every read that returns more than one expired shard.

This implementation satisfies the following constraints:

* Supports arbitrary order-free data structures [CRDT](http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf)s
* Counters can be read/updated on as many nodes as we like
* Updates never read from Cassandra
* All writes to Cassandra are idempotent
* Mutator memory can be limited without affecting updates

Limitations:

* Reads (and GC) slow linearly with the number of mutators
* GC requires all replicas to be up and reads at CL `ALL`

Requirements
------------

* JRuby 1.7.4
* [stream-lib](https://github.com/clearspring/stream-lib)
* Apache Cassandra 1.2

Running
-------

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
-------

Public domain, not fit for any particular purpose.



