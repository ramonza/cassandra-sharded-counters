require_relative '../../bootstrap'
require 'rack/test'
require 'minitest/autorun'
require 'api'
require 'json'

class TestApi < MiniTest::Unit::TestCase

  include Rack::Test::Methods

  def app
    ShardedCounters::API
  end

  def setup
    delete '/approx_distinct'
    assert_ok
    delete '/sum'
    assert_ok
  end

  def test_get_empty
    get '/approx_distinct/unknown-key-xyz'
    assert_ok
    assert_equal Hash.new, JSON.parse(last_response.body)
  end

  def test_add_and_get
    post '/approx_distinct/0/0', {value: 10}
    assert_ok
    get '/approx_distinct/0'
    assert_ok
    assert_equal({'0' => 1}, JSON.parse(last_response.body))
  end

  def test_add_sequence_and_get
    post '/sum/foo/bar/add-sequence', {range_start: 1, range_end: 100}
    assert_ok
    post '/sum/foo/baz/add-sequence', {range_start: 1, range_end: 50}
    assert_ok
    get '/sum/foo'
    assert_ok
    values = JSON.parse(last_response.body)
    assert_equal 5050, values['bar']
    assert_equal 1275, values['baz']
  end

  def test_clear_cache
    post '/sum/foo/bar/add-sequence', {range_start: 1, range_end: 100}
    assert_ok
    delete '/sum/cache'
    assert_ok
    post '/sum/foo/baz/add-sequence', {range_start: 1, range_end: 50}
    assert_ok
    get '/sum/foo'
    assert_ok
    values = JSON.parse(last_response.body)
    assert_equal 5050, values['bar']
    assert_equal 1275, values['baz']
  end

  private

  def assert_ok
    status = last_response.status
    assert (200..299).include?(status), "Request: #{last_request.inspect}, Response: #{last_response.inspect}"
  end

end