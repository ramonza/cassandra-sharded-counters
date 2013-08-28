require 'cql'

module CqlHelper

  @client = Cql::Client.connect(host: 'localhost')
  @client.use('counters')

  def self.client
    @client
  end

  class Blob
    def initialize(buffer)
      @buffer = buffer
    end

    def to_cql
      "0x#{@buffer.unpack('H*')[0]}"
    end
  end

  def self.quote_cql_string(str)
    "'" + str.gsub(/'/, "''") + "'"
  end

  def self.quote_cql_param(value)
    return value.to_cql if value.respond_to? :to_cql
    case value
      when Numeric, true, false
        value.to_s
      when String, Symbol
        quote_cql_string(value.to_s)
      when Time
        (value.to_f * 1000).to_i.to_s
      else
        raise "Don't know how to convert #{value} to CQL"
    end
  end

  def self.query(cql, params = {}, consistency = :one)
    interpolated = interpolate_cql(cql, params)
    if debug_cql?
      $stderr.puts "CQL : #{interpolated}"
      $stderr.flush
    end
    begin
      client.execute(interpolated, consistency)
    rescue => e
      raise "Error executing CQL '#{interpolated}': #{e.message}"
    end
  end

  def self.debug_cql?
    @debug_cql = ENV['DEBUG_CQL'].to_i == 1 if @debug_cql.nil?
    @debug_cql
  end

  def self.update(table, keys, values)
    set = values.map { |name, value| "#{name} = #{quote_cql_param(value)}" }.join(', ')
    where = keys.map { |name, value| "#{name} = #{quote_cql_param(value)}" }.join(' AND ')
    execute("UPDATE #{table} SET #{set} WHERE #{where}")
  end

  class << self
    alias_method :execute, :query
  end

  def self.interpolate_cql(cql, params = {})
    params = Hash[params.map { |name, value| [name, quote_cql_param(value)] }]
    cql % params
  end

  def self.execute_batch(statements)
    interpolated = statements.map{ |statement| interpolate_cql(*statement) }
    batch_statement = ['BEGIN BATCH', interpolated, 'APPLY BATCH'].flatten.join("\n")
    execute(batch_statement)
  end

end