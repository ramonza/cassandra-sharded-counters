require_relative '../../bootstrap'
require 'minitest/autorun'
require 'simple_counters'
require 'approx_distinct_counter'

class CounterTest < MiniTest::Unit::TestCase
  def do_test_counter(factory)
    counter = factory.new

    (0..999).each do |n|
      counter.update(n)
    end

    shards = (0..9).collect do |i|
      factory.new.tap do |shard|
        start = 100 * i
        (start..start+99).each { |n| shard.update(n) }
      end
    end
    union = factory.merge(shards)
    assert_equal counter.value, union.value

    blobs = shards.collect(&:serialize)
    shards = blobs.collect{ |blob| factory.deserialize(blob) }
    union = factory.merge(shards)
    assert_equal  counter.value, union.value
  end

  def test_all_counters
    do_test_counter(ApproxDistinctCounter)
    do_test_counter(SumCounter)
    do_test_counter(MinCounter)
    do_test_counter(MaxCounter)
  end
end