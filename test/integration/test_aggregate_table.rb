require_relative 'helper'
require 'minitest/autorun'
require 'aggregate_table'
require 'simple_counters'
require 'ostruct'

class TestAggregateTable < MiniTest::Unit::TestCase

  ROW = 'row1'
  COLUMN = 'column1'

  def setup
    AggregateTable.reset_time
  end

  def teardown
    AggregateTable.reset_time
  end

  def test_basic_increment
    table = AggregateTable.new('test', SumCounter)
    table.create_table
    table.update(ROW, COLUMN, 10)
    assert_equal 10, table.read_row(ROW)[COLUMN]
    table.update(ROW, COLUMN, 20)
    assert_equal 30, table.read_row(ROW)[COLUMN]
  end

  def test_eviction
    table = AggregateTable.new('test', SumCounter)
    table.create_table
    table.update(ROW, COLUMN, 10)
    assert_equal 10, table.read_row(ROW)[COLUMN]
    table.clear_cache
    table.update(ROW, COLUMN, 20)
    assert_equal 30, table.read_row(ROW)[COLUMN]
  end

  def test_garbage_collection
    table = AggregateTable.new('test', SumCounter)
    table.create_table

    gc = nil
    time = Time.now
    table.define_singleton_method(:on_garbage_collection) do |the_gc|
      gc = the_gc
    end
    table.define_singleton_method(:time_now) { time }

    table.update(ROW, COLUMN, 10)
    assert_equal 10, table.read_row(ROW)[COLUMN]
    table.clear_cache

    table.update(ROW, COLUMN, 20)
    assert_equal 30, table.read_row(ROW)[COLUMN]
    table.clear_cache

    assert_nil gc, 'No GC yet'

    time += 3.hours
    assert_equal 30, table.read_row(ROW)[COLUMN]
    assert gc, 'GC has occurred'
    assert_equal 2, gc.collecting.size

  end

end