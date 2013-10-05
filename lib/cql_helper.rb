require 'cql'

module CqlHelper

  @client = Cql::Client.connect(host: 'localhost')
  @client.use('counters')

  def client
    CqlHelper.instance_variable_get :@client
  end

  class Blob
    def initialize(buffer)
      @buffer = buffer
    end

    def to_cql
      "0x#{@buffer.unpack('H*')[0]}"
    end
  end

  def quote_cql_string(str)
    "'" + str.gsub(/'/, "''") + "'"
  end

  def quote_cql_param(value)
    return value.to_cql if value.respond_to? :to_cql
    case value
      when Numeric, true, false
        value.to_s
      when String, Symbol
        quote_cql_string(value.to_s)
      when java.util.UUID
        value.to_s
      when Cql::Uuid
        value.to_s
      else
        raise "Don't know how to convert #{value} to CQL"
    end
  end

  def debug_cql?
    @debug_cql = ENV['DEBUG_CQL'].to_i == 1 if @debug_cql.nil?
    @debug_cql
  end

  def update(table, keys, values)
    set = values.map { |name, value| "#{name} = #{quote_cql_param(value)}" }.join(', ')
    where = keys.map { |name, value| "#{name} = #{quote_cql_param(value)}" }.join(' AND ')
    execute("UPDATE #{table} SET #{set} WHERE #{where}")
  end

  def query(cql, params = {}, consistency = :one)
    execute_internal(cql, params, consistency).map &:with_indifferent_access
  end

  def execute(cql, params = {}, consistency = :one)
    execute_internal(cql, params, consistency)
    nil
  end

  def interpolate_cql(cql, params = {})
    params = Hash[params.map { |name, value| [name.to_sym, quote_cql_param(value)] }]
    begin
      cql % params
    rescue KeyError => e
      raise KeyError, "#{e}: #{cql}"
    end
  end

  def execute_batch(statements, timestamp_seconds = nil)
    if timestamp_seconds
      begin_batch = "BEGIN BATCH USING TIMESTAMP #{(timestamp_seconds * 1_000_000).to_i}"
    else
      begin_batch = 'BEGIN BATCH'
    end
    interpolated = statements.map { |statement| interpolate_cql(*statement) }
    batch_statement = [begin_batch, interpolated, 'APPLY BATCH'].flatten.join("\n")
    execute(batch_statement)
  end

  private

  def execute_internal(cql, params, consistency)
    interpolated = interpolate_cql(cql, params)
    if debug_cql?
      $stderr.puts "CQL : #{interpolated}"
      $stderr.flush
    end
    begin
      client.execute(interpolated, consistency)
    rescue => e
      raise "#{e.message} in CQL: '#{interpolated}'"
    end
  end

end