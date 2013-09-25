require 'cql_helper'

class GarbageCollector

  include CqlHelper
  TALLY = java.util.UUID.from_string('e94b067d-0d8f-4efb-be3a-9518b961fc81')

  attr_reader :all_shards, :collecting, :new_tally

  def initialize(table, key)
    @table = table
    @key = key
  end

  def collect
    @all_shards = find_shards
    @collecting = collectable(@all_shards)
    return if @collecting.count <= 1

    @new_tally = @table.merge_shards(@collecting)
    non_tally = collecting.reject { |shard| shard[:mutator_id].to_s == TALLY.to_s }

    # delete expired shards
    batch = non_tally.collect { |shard| [ shard_delete_query, shard ] }

    # update the tally
    batch << [ shard_update_query, @key.merge(mutator_id: TALLY, state: CqlHelper::Blob.new(@new_tally.serialize)) ]

    timestamp = non_tally.collect { |shard| shard[:expires_at] }.max + 1.second
    execute_batch(batch, timestamp)
  end

  def can_collect?(shards)
    collectable(shards).size >= 1
  end

  private

  def collectable(shards)
    cutoff = @table.time_now - 1.hour
    shards.select { |shard| !shard[:expires_at] || shard[:expires_at] < cutoff }
  end

  def shard_update_query
    "UPDATE #{@table.name} SET state = %{state} WHERE row_key = %{row_key} AND column_key = %{column_key} AND mutator_id = %{mutator_id}"
  end

  def shard_delete_query
    "DELETE FROM #{@table.name} WHERE row_key = %{row_key} AND column_key = %{column_key} AND mutator_id = %{mutator_id} "
  end

  def find_shards
    query("SELECT * FROM #{@table.name} WHERE row_key = %{row_key} AND column_key = %{column_key}", @key, :all)
  end
end