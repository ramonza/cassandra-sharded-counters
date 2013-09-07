require 'cql_helper'
require 'counter'
require 'socket'
require 'forwardable'
require 'mutator'

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

  def name
    @table_name
  end

  def read_row(row_key)
    results = query("SELECT * FROM #{name} WHERE row_key = %{row_key}", row_key: row_key)
    report = Hash.new
    results.group_by {|result| result['column_key']}.each do |column_key, entry|
      report[column_key] = merge_shards(entry).value
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

  private

  def merge_shards(shards)
    counters = shards.map {|shard| @factory.deserialize(shard['state']) }
    @factory.merge(counters)
  end

  extend Forwardable

  def_delegator :@factory, :new, :new_counter
end
