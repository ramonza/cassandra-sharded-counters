require 'java'
require 'thread'
require 'counter'

class ApproxDistinctCounter < Counter
  import 'com.clearspring.analytics.stream.cardinality.HyperLogLog'

  attr_reader :impl

  def initialize(impl=nil)
    @impl = impl || HyperLogLog.new(10)
  end

  def self.deserialize(state)
    self.new HyperLogLog::Builder.build(state.to_java_bytes)
  end

  def self.merge(others)
    new.tap do |result|
      result.merge! others
    end
  end

  def serialize
    impl.bytes.to_s
  end

  def update(value)
    impl.offer(value)
  end

  def merge!(others)
    others.each do |other|
      impl.add_all other.impl
    end
  end

  def value
    impl.cardinality
  end
end