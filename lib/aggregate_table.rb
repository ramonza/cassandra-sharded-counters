require 'cql_helper'
require 'counter'
require 'socket'
require 'forwardable'
require 'mutator'
require 'garbage_collector'

# Wraps an aggregate table in Cassandra. The table is logically keyed by (+row_key+, +column_key+). There are
# three additional components of the primary key used to separate the counters into locally consistent shards:
# 1. +host_id+ - opaque integer value identifying an distinct entity or 0
# 2. +run+ - monotonically increasing (per host_id) sequence or 0
# 3. +generation+ - opaque integer value or 0
# For an entry <tt>(host_id, run, generation, final, current_state)</tt>, +current_state+ is _final_ if
# 1. The entry <tt>(host_id, current_run)</tt> appears in the +host_current_runs+ table, where <tt>current_run > run</tt>
# 2. +final+ has the value +true+
# Final values will never be updated so it is safe for any actor who determines that a state is final to merge
# all the final states for a given <tt>(row_key, column_key)</tt> by inserting the merged value and deleting the old values
# atomically (they should have the same partition key).
class AggregateTable

  include CqlHelper

  def initialize(table_name, factory)
    @table_name, @factory = table_name, factory
    @cache = Hash.new
    @update_access = Mutex.new
  end

  def create_table
    begin
      execute("DROP TABLE #{name}")
    rescue
      # ignored
    end
    execute(<<-EOF)
      CREATE TABLE #{name} (
        row_key varchar,
        column_key varchar,
        mutator_id uuid,
        state blob,
        expires_at timestamp,
        PRIMARY KEY (row_key, column_key, mutator_id)
      )
    EOF
  end

  def name
    @table_name
  end

  def read_row(row_key)
    results = query("SELECT * FROM #{name} WHERE row_key = %{row_key}", row_key: row_key)
    report = Hash.new
    results.group_by {|result| result[:column_key]}.each do |column_key, shards|
      report[column_key] = merge_shards(shards).value
      gc = GarbageCollector.new(self, row_key: row_key, column_key: column_key)
      if gc.collectable(shards).size >= 1
        gc.collect
      end
    end
    report
  end

  def clear! # this is just for testing
    @update_access.synchronize do
      execute("TRUNCATE #{name}")
      @cache = Hash.new
    end
  end

  # Clear the internal cache. This could be done on a LRU basis. The only consequence of clearing the cache is shard
  # proliferation, it does not slow updates.
  def clear_cache
    @update_access.synchronize do
      @cache.values.each &:flush
      @cache = Hash.new
    end
  end

  def update(row_key, column_key, value)
    @update_access.synchronize do
      key = {row_key: row_key, column_key: column_key}
      begin
        (@cache[key] ||= Mutator.new(self, key)).increment(value)
      rescue Mutator::DeadMutatorException
        @cache.delete(key)
        retry
      end
    end
  end

  def time
    Time.now
  end

  def merge_shards(shards)
    counters = shards.map {|shard| @factory.deserialize(shard[:state]) }
    @factory.merge(counters)
  end

  def on_garbage_collection(all_shards, collected_shards, tally_counter)
    $logger.debug("Garbage collected #{collected_shards.size}/#{all_shards.size} shards, replaced with new value #{tally_counter.value}")
  end

  extend Forwardable

  def_delegator :@factory, :new, :new_counter

  protected
end
