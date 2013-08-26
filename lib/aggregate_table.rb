require 'cql'
require 'singleton'
require 'counter'
require 'socket'

# Store counters in Cassandra. Each server instance has a unique +shard_id+ which
# it is responsible for. We guarantee local consistency between values with the same +shard_id+
# so that we can just push new values to Cassandra without first reading existing values.
# When we come to query the total value of the counters, we combine all these shard-local values.
class AggregateTable

  @client = Cql::Client.connect(host: 'localhost')
  @client.use('counters')
  @shard_id = ENV['SHARD_ID'] || "#{Socket.gethostname}-#{Process.pid}"

  def self.cql
    @client
  end

  def self.shard_id
    @shard_id
  end

  attr_reader :table_name

  def initialize(table_name, factory)
    @table_name, @factory = table_name, factory
    @save_statement = self.class.cql.prepare("UPDATE #{table_name} SET counter_state = ? WHERE row_key = ? AND column_key = ? AND shard_id = ?")
    @select_row = self.class.cql.prepare("SELECT * FROM #{table_name} WHERE row_key = ?")
    @retrieve_statement = self.class.cql.prepare("SELECT * FROM #{table_name} WHERE row_key = ? AND column_key = ? AND shard_id = ?")
    @cache = Java::JavaUtilConcurrent::ConcurrentHashMap.new
  end

	def read_row(row_key)
    by_column = @select_row.execute(row_key, :one).group_by { |row| row['column_key'] }
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
		self.class.cql.execute("TRUNCATE #{table_name}", :all)
  end

	def get_for_update(row_key, column_key)
		key = [row_key, column_key]
		result = @cache[key]
		unless result
			counter = CounterHolder.new(self, key)
			result = @cache.put_if_absent(key, counter) || counter
		end
		result.fetch!
		result
  end

  def retrieve(key)
    row_key, column_key = key
    row = @retrieve_statement.execute(row_key, column_key, self.class.shard_id, :one)
    if row.empty?
      @factory.new
    else
      @factory.deserialize(row.first['counter_state'])
    end
  end

  def store(key, counter)
    row_key, column_key = key
    @save_statement.execute(counter.serialize, row_key, column_key, self.class.shard_id, :one)
  end
end

# An in-memory holder for a Counter. Note the absence of a read method.
class CounterHolder
  def initialize(store, key)
    @store, @key = store, key
    @counter = nil
    @mutex = Mutex.new
  end

  def fetch!
    lock do
      @counter = @store.retrieve(@key) unless @counter
    end
  end

  def save
    lock do
      @store.store(@key, @counter)
    end
  end

  def update(item)
    lock do
      @counter.update(item)
    end
  end

  private
  def lock
    @mutex.synchronize { yield }
  end
end