require 'java'
require 'cql_helper'

class Mutator

  include CqlHelper

  class DeadMutatorException < StandardError; end

  MINUTES = 60
  HOURS = 60 * MINUTES

  def initialize(table, key)
    @table = table
    @shard_key = key.merge(mutator_id: java.util.UUID.randomUUID)
    @counter = @table.new_counter
    @deathdate = round_to_next_minute(Time.now + 24 * HOURS + 1 * MINUTES)
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
      raise DeadMutatorException, "Died at #{@deathdate}"
    end
  end

  def dead?
    @deathdate < Time.now + 1 * HOURS
  end

  def flush
    @deathdate = round_to_next_minute(Time.now + 60 * MINUTES)
    save
  end

  private

  def save
    update(@table.name, @shard_key, deathdate: @deathdate, state: @state) if @state
  end

  def round_to_next_minute(t)
    seconds = t.to_i
    unless seconds % MINUTES == 0
      seconds = seconds + 1 * MINUTES
      seconds = seconds - (seconds % MINUTES)
    end
    Time.at(seconds)
  end
end