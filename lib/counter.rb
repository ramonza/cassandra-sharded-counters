# +Counter+ is a single piece of state storing an aggregate value. It can be updated as new values come in,
# +serialize+d to a string, and +merge+d with other counters of the same type.
class Counter

  # +merge+ the given counters. This operation must be associative.
  def self.merge(counters)
    raise 'Subclass responsibility'
  end

  # +deserialize+ from the given string that was produced by a previous call to +serialize+
  def self.deserialize(blob)
    raise 'Subclass responsibility'
  end

  # +serialize+ to a string
  def serialize
    raise 'Subclass responsibility'
  end

  # +update+ the internal state of the counter with the next +value+
  def update(value)
    raise 'Subclass responsibility'
  end

  # read the current +value+ of this counter
  def value
    raise 'Subclass responsibility'
  end
end

class SimpleCounter < Counter

  def self.merge(counters)
    new merge_values(counters.map(&:value).compact)
  end

  def self.deserialize(blob)
    if blob.empty?
      new
    else
      new(blob.to_i)
    end
  end

  def self.merge_values(values)
    raise 'Subclass responsibility'
  end

  def initialize(state=nil)
    @value = state || default_value
  end

  def serialize
    value.to_s
  end

  def update(new_value)
    if @value
      @value = self.class.merge_values([@value, new_value.to_i])
    else
      @value = new_value
    end
  end

  def default_value
    nil
  end

  def value
    @value
  end
end