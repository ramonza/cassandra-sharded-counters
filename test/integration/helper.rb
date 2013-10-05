require_relative '../../bootstrap'
require 'aggregate_table'

class AggregateTable
  @starttime = Time.now.to_i

  alias_method :old_time, :timestamp_seconds

  def timestamp_seconds
    self.class.timestamp_seconds
  end

  def self.timestamp_seconds
    @faketime
  end

  def self.advance_clock(duration)
   @faketime += duration.to_i
  end

  def self.reset_clock
    @faketime = @starttime
  end
end

AggregateTable.reset_clock
