require 'cql_helper'
require 'counter'
require 'socket'
require 'forwardable'

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

  extend Forwardable

  @host_id = (ENV['HOST_ID'] || Socket.gethostname.hash).to_i
  @current_run = (ENV['RUN_ID'] || 1).to_i

  class << self
    attr_reader :host_id, :current_run
  end

  delegate [:host_id, :current_run] => 'self.class'
  attr_reader :table_name

  def initialize(table_name, factory)
    @table_name, @factory = table_name, factory
    @cache = Hash.new
    @current_generation = 1
    @update_access = Mutex.new
    start_run
  end

	def read_row(row_key)
    results = CqlHelper.query("SELECT * FROM #{table_name} WHERE row_key = %{row}", {row: row_key})
    by_column = results.group_by { |row| row['column_key'] }
    report = Hash.new
    by_column.each do |column_key, rows|
      shards = rows.map { |row|
        @factory.deserialize(row['counter_state'])
      }
      sum = @factory.merge(shards).value
      report[column_key] = sum
    end
    report
  end

	def clear! # this is just for testing
    @update_access.synchronize do
      CqlHelper.execute("TRUNCATE #{table_name}")
      @cache = Hash.new
      @current_generation = 1
    end
  end

  # Clear the internal cache. This could be done on a LRU basis. The only consequence of clearing the cache is shard
  # proliferation.
  def clear_cache
    @update_access.synchronize do
      @cache.each do |key, accumulator|
        store(key, accumulator.generation, accumulator.serialize, true)
      end
      @cache = Hash.new
      @current_generation += 1
    end
  end

	def update(row_key, column_key, value)
    @update_access.synchronize do
		  key = AggregateKey.new(row_key, column_key)
      accumulator = (@cache[key] ||= Accumulator.new(@current_generation, new_counter))
      accumulator.update(value)
      store(key, accumulator.generation, accumulator.serialize)
    end
  end

  private

  def store(key, generation, state, finalize = false)
    key_columns = {
        row_key: key.row,
        column_key: key.column,
        host_id: host_id,
        run: current_run,
        generation: generation
    }
    updates = {
        counter_state: CqlHelper::Blob.new(state)
    }
    updates[:final] = true if finalize
    CqlHelper.update(table_name, key_columns, updates)
  end

  def start_run
    sanity_check!
    CqlHelper.update('host_current_runs', {host_id: host_id, aggregate_table: table_name}, {current_run: current_run, started_at: Time.now})
  end

  def sanity_check!
    previous_runs = CqlHelper.query('SELECT current_run FROM host_current_runs WHERE aggregate_table = %{aggregate_table} AND host_id = %{host_id}',
                    host_id: host_id, aggregate_table: table_name)
    return unless previous_runs.any?
    raise 'Previous run greater than current run' if previous_runs.first.run >= current_run
  end

  def_delegator :@factory, :new, :new_counter
end

class AggregateKey < Struct.new(:row, :column)
end

# An in-memory holder for a Counter that accumulates updates.
class Accumulator < Struct.new(:generation, :counter)
  extend Forwardable
  delegate [:serialize, :update] => :counter
end