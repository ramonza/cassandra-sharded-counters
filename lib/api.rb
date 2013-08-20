
require 'json'
require 'time'

require 'counter'
require 'counter_store'
require 'approx_distinct_counter'
require 'simple_counters'

class API < Grape::API

  TYPES = {
      approx_distinct: ApproxDistinctCounter,
      sum: SumCounter,
      min: MinCounter,
      max: MaxCounter,
  }

  TYPES.each do |name, counter_type|

    store = CounterStore.new(name, counter_type)

    resource name do
      helpers do
        def time
          @time ||= (params[:time] || Time.now)
        end
        def hour
          @hour ||= time.strftime("%H").to_i
        end
        def day
          @day ||= time.strftime("%Y%m%d")
        end
      end

      desc 'Add a single value'
      params do
        optional :time, type: Time
        requires :value, type: Integer, desc: 'The value to add'
      end
      post 'add/:value' do
        value = params[:value]
        counter = store.get_for_update(day, hour)
        counter.update(value)
        counter.save
        "OK: #{value}\n"
      end

      desc 'Add a sequence of values'
      params do
        optional :time, type: Time
        requires :range_start, type: Integer, desc: 'Start of the range'
        requires :range_end, type: Integer, desc: 'End of the range'
      end
      post 'add-sequence' do
        min, max = %w(range_start range_end).map { |k| params[k].to_i }
        to_save = Set.new
        (min..max).each do |value|
          counter = store.get_for_update(day, hour)
          counter.update(value)
          to_save << counter
        end
        to_save.each &:save
        'OK'
      end

      desc 'Produce an hourly summary'
      params do
        optional :time, type: Time
      end
      get 'hourly-summary' do
        result = store.read_hourly_counters(day)
        JSON.pretty_generate(result)
      end

      desc 'Remove all values'
      post 'reset' do
        store.reset
      end
    end

  end
end
