
require 'json'
require 'time'

require 'counter'
require 'aggregate_table'
require 'approx_distinct_counter'
require 'simple_counters'

module ShardedCounters
  class API < Grape::API

    default_format :json

    TYPES = {
        approx_distinct: ApproxDistinctCounter,
        sum: SumCounter,
        min: MinCounter,
        max: MaxCounter,
    }

    TYPES.each do |name, counter_type|

      store = AggregateTable.new(name, counter_type)

      resource name do

        desc 'Add a single value'
        params do
          requires :value, type: Integer, desc: 'The value to add'
        end
        post ':row_key/:column_key' do
          value = params[:value]
          counter = store.get_for_update(params[:row_key], params[:column_key])
          counter.update(value)
          counter.save
          nil
        end

        desc 'Add a sequence of values'
        params do
          requires :range_start, type: Integer, desc: 'Start of the range'
          requires :range_end, type: Integer, desc: 'End of the range'
        end
        post ':row_key/:column_key/add-sequence' do
          to_save = Set.new
          (params[:range_start]..params[:range_end]).each do |value|
            counter = store.get_for_update(params[:row_key], params[:column_key])
            counter.update(value)
            to_save << counter
          end

          to_save.each &:save
          nil
        end

        desc 'Read values in a row'
        get ':row_key' do
          store.read_row(params[:row_key])
        end

        desc 'Remove all values'
        delete do
          store.reset
          nil
        end
      end

    end
  end
end