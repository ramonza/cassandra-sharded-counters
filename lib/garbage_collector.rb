require 'cql_helper'

class GarbageCollector

  include CqlHelper

  TALLY = java.util.UUID.from_string('e94b067d-0d8f-4efb-be3a-9518b961fc81') # just a random, pre-generated UUID
  GC_LAG_SECONDS = 24.hours.to_i # we will only collect shards with expires_at older than this

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
    tally_state = CqlHelper::Blob.new(@new_tally.serialize)
    batch << [ shard_update_query, @key.merge(expires_at: 0, mutator_id: TALLY, state: tally_state) ]

    timestamp = non_tally.collect { |shard| shard[:expires_at] }.max
    execute_batch(batch, timestamp)
  end

  def can_collect?(shards)
    collectable(shards).size >= 1
  end

  private

  def collectable(shards)
    cutoff = @table.timestamp_seconds - GC_LAG_SECONDS
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