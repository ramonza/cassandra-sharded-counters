require 'cql_helper'
require 'counter'
require 'socket'
require 'forwardable'

# Store counters in Cassandra. Each server instance has a unique +shard_id+ which
# it is responsible for. We guarantee local consistency between values with the same +shard_id+
# so that we can just push new values to Cassandra without first reading existing values.
# When we come to query the total value of the counters, we combine all these shard-local values.
class AggregateTable

  extend Forwardable

  @host_id = ENV['HOST_ID'] || Socket.gethostname.hash
  @current_run = ENV['RUN_ID'] || 1

  class << self
    attr_reader :host_id, :current_run
  end

  delegate [:host_id, :current_run] => 'self.class'
  attr_reader :table_name

  def initialize(table_name, factory)
    @table_name, @factory = table_name, factory
    @cache = Java::JavaUtilConcurrent::ConcurrentHashMap.new
    @generation = Java::JavaUtilConcurrentAtomic::AtomicInteger.new(1)
    start_run
  end

	def read_row(row_key)
    results = CqlHelper.query("SELECT * FROM #{table_name} WHERE row_key = %{row}", {row: row_key})
    by_column = results.group_by { |row| row['column_key'] }
    entries = by_column.collect do |row_key, rows|
      shards = rows.map { |row|
        @factory.deserialize(row['counter_state'])
      }
      sum = @factory.merge(shards).value
      [row_key, sum]
    end
    Hash[entries]
  end

	def reset
		@cache.clear
		CqlHelper.execute("TRUNCATE #{table_name}", {}, :all)
  end

	def get_for_update(row_key, column_key)
		key = [row_key, column_key]
		result = @cache[key]
		unless result
			counter = Accumulator.new(self, row_key, column_key, @generation.get)
			result = @cache.put_if_absent(key, counter) || counter
		end
		result
  end

  def retrieve(key)
    row_key, column_key = key
    row = CqlHelper.query("SELECT * FROM #{table_name} WHERE row_key = %{row} AND column_key = %{column} AND shard_id = %{shard_id}",
                          row: row_key, column: column_key, shard_id: self.class.shard_id)
    if row.empty?
      @factory.new
    else
      @factory.deserialize(row.first['counter_state'])
    end
  end

  def store(entry, state)
    key = {
        row_key: entry.row_key,
        column_key: entry.column_key,
        host_id: host_id,
        run: current_run,
        generation: entry.generation
    }
    CqlHelper.update(table_name, key, {counter_state: state})
  end

  private

  def start_run
    sanity_check!
    CqlHelper.update('host_current_runs', {host_id: host_id, aggregate_table: table_name}, {current_run: current_run})
  end

  def sanity_check!
    previous_runs = CqlHelper.query('SELECT current_run FROM host_current_runs WHERE aggregate_table = %{aggregate_table} AND host_id = %{host_id}',
                    host_id: host_id, aggregate_table: table_name)
    return unless previous_runs.any?
    raise 'Previous run greater than current run' if previous_runs.first.run >= current_run
  end

  def_delegator :@factory, :new, :new_counter
end

# An in-memory holder for a Counter that accumulates updates.
class Accumulator

  attr_reader :row_key, :column_key, :generation

  def initialize(store, row_key, column_key, generation)
    @store, @row_key, @column_key, @generation = store, row_key, column_key, generation
    @counter = store.new_counter
    @mutex = Mutex.new
  end

  def save
    state = lock { @counter.serialize }
    @store.store(self, CqlHelper::Blob.new(state))
  end

  def update(item)
    lock { @counter.update(item) }
  end

  private
  def lock
    @mutex.synchronize { yield }
  end
end