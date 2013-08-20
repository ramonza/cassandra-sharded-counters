class Counter

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