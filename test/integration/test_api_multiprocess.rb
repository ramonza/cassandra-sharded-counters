require 'minitest/autorun'
require 'net/http'
require 'json'
require 'pry'

class TestApiMultiprocess < MiniTest::Unit::TestCase

	def test_approx_distinct
    @counter = 'approx_distinct'
    @time = '2010-01-01+09:00'

    all_instances do |port|
      Net::HTTP.post_form(URI("http://localhost:#{port}/#{@counter}/reset"), {})
    end

		post_sequence_on('8002', 1, 1000)
		result = read_count_from('8003')
		assert_in_epsilon 1000, result, 0.1

    post_sequence_on('8004', 1001, 5000)
		
		result = read_count_from('8003')
		assert_in_epsilon 5000, result, 0.1
		
		result = read_count_from('8005')
		assert_in_epsilon 5000, result, 0.1

		all_instances do |port|
			post_sequence_on(port, 1, 5000)
		end
		result = read_count_from('8006')
		assert_in_epsilon 5000, result, 0.1
	end

	def all_instances 
		(8001..8009).each do |n|
			yield n
		end
	end

	def post_sequence_on(port, range_start, range_end)
    uri = URI("http://localhost:#{port}/#{@counter}/add-sequence?time=#{@time}")
    Net::HTTP.post_form(uri, range_start: range_start, range_end: range_end)
  end

	def read_count_from(port)
    uri = URI("http://localhost:#{port}/#{@counter}/hourly-summary?time=#{@time}")
    response = Net::HTTP.get(uri)
    JSON.parse(response)['9']
  end

end
