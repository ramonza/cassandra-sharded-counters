require 'cql_helper'
require 'counter'
require 'socket'
require 'forwardable'
require 'mutator'
require 'garbage_collector'

# Wraps an aggregate table in Cassandra. Each (+row_key+, +column_key+) hosts a single counter.
# Dispatches to +Mutator+ for mutations and +GarbageCollector+ for garbage collection.
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
    results.group_by { |result| result[:column_key] }.each do |column_key, shards|
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
    counters = shards.map { |shard| @factory.deserialize(shard[:state]) }
    @factory.merge(counters)
  end

  def on_garbage_collection(all_shards, collected_shards, tally_counter)
    $logger.debug("Garbage collected #{collected_shards.size}/#{all_shards.size} shards, replaced with new value #{tally_counter.value}")
  end

  extend Forwardable

  def_delegator :@factory, :new, :new_counter
end
