require 'java'
require 'cql_helper'

class Mutator

  include CqlHelper

  # Assume that clocks are synchronized to within this margin
  CLOCK_DRIFT_ALLOWANCE = 1.hour

  class DeadMutatorException < StandardError;
  end

  def initialize(table, key)
    @table = table
    @shard_key = key.merge(mutator_id: java.util.UUID.randomUUID)
    @counter = @table.new_counter
    @expires_at = (@table.timestamp_seconds + 24.hours).to_i
  end

  def increment(value)
    ensure_alive!
    @counter.update(value)
    @state = CqlHelper::Blob.new(@counter.serialize)
    save
  end

  def ensure_alive!
    if dead?
      @counter = @shard_key = nil
      raise DeadMutatorException, "Died at #{@expires_at}"
    end
  end

  def dead?
    @expires_at < earliest_expiry_time
  end

  # This mutator is being flushed out of memory
  def flush
    @expires_at = earliest_expiry_time
    save
  end

  private

  def save
    update(@table.name, @shard_key, expires_at: @expires_at, state: @state) if @state
  end

  # The earliest time in the future where this mutator is still allowed to issue updates
  def earliest_expiry_time
    (@table.timestamp_seconds + CLOCK_DRIFT_ALLOWANCE).to_i
  end
end