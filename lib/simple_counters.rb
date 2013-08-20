require 'counter'

class SumCounter < SimpleCounter
  def self.merge_values(values)
    values.reduce(0, :+)
  end
end

class CountCounter < SumCounter
  def update(value)
    super(1)
  end
end

class ProductCounter < SimpleCounter
  def self.merge_values(values)
    values.reduce(1, :*)
  end
end

class MinCounter < SimpleCounter
  def self.merge_values(values)
    values.min
  end
end

class MaxCounter < SimpleCounter
  def self.merge_values(values)
    values.max
  end
end