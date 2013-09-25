require 'java'
require 'cql_helper'

class Mutator

  include CqlHelper

  class DeadMutatorException < StandardError;
  end

  def initialize(table, key)
    @table = table
    @shard_key = key.merge(mutator_id: java.util.UUID.randomUUID)
    @counter = @table.new_counter
    @expires_at = @table.time_now + 24.hours
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
    @expires_at < @table.time_now + 1.hour
  end

  def flush
    @expires_at = @table.time_now + 1.hour
    save
  end

  private

  def save
    update(@table.name, @shard_key, expires_at: @expires_at, state: @state) if @state
  end
end