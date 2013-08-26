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
    end
  end

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

  private

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