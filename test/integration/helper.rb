require_relative '../../bootstrap'
require 'aggregate_table'

class AggregateTable
  @starttime = Time.now

  alias_method :old_time, :time

  def time
    self.class.time
  end

  def self.time
    @faketime
  end

  def self.set_time(time)
   @faketime = time
  end

  def self.reset_time
    @faketime = @starttime
  end
end

AggregateTable.reset_time
