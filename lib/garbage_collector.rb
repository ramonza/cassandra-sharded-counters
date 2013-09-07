require 'cql_helper'

class GarbageCollector

  include CqlHelper
  TALLY = java.util.UUID.from_string('e94b067d-0d8f-4efb-be3a-9518b961fc81')

  def initialize(table, key)
    @table = table
    @key = key
  end

  def collect
    all_shards = query("SELECT * FROM #{@table.name} WHERE row_key = %{row_key} AND column_key = %{column_key}", @key, :all)
    collecting = collectable(all_shards)
    return false if collecting.count <= 1
    tally = @table.merge_shards(collecting)
    timestamp = collecting.map { |shard| shard[:expires_at] }.compact.max + 1.second
    batch = Array.new
    collecting.each do |to_delete|
      if to_delete['mutator_id'].to_s != TALLY.to_s
        batch << [
          "DELETE FROM #{@table.name} WHERE row_key = %{row_key} AND column_key = %{column_key} " +
          "AND mutator_id = %{mutator_id} /*state = %{state}*/",
          to_delete.merge(state: CqlHelper::Blob.new(to_delete['state']))
        ]
      end
    end
    batch << [
      "UPDATE #{@table.name} SET state = %{state} WHERE row_key = %{row_key} AND column_key = %{column_key} AND mutator_id = %{mutator_id}",
      @key.merge(mutator_id: TALLY, state: CqlHelper::Blob.new(tally.serialize))
    ]
    @table.on_garbage_collection(all_shards, collecting, tally)
    execute_batch(batch, timestamp)
  end

  def collectable(shards)
    cutoff = @table.time - 1.hour
    shards.select { |shard| !shard[:expires_at] || shard[:expires_at] < cutoff }
  end
end