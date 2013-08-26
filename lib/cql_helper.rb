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
    return value.to_s if value.is_a? Numeric
    return quote_cql_string(value.to_s) if value.is_a?(String) || value.is_a?(Symbol)
    return value.to_cql if value.respond_to? :to_cql
    raise "Don't know how to convert #{value} to CQL"
  end

  def self.query(cql, params = {}, consistency = :one)
    interpolated = interpolate_cql(cql, params)
    $stderr.puts "CQL : #{interpolated}" if debug_cql?
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

  def self.batch
    statements = ['BEGIN BATCH']
    while (next_statement = yield)
      statements << interpolate_cql(*next_statement)
    end
    statements << 'APPLY BATCH'
    client.execute(statements.join("\n"))
  end

end