require 'rake/testtask'
require_relative 'bootstrap'
require 'cql'

BASEDIR = File.dirname(__FILE__)

Rake::TestTask.new do |t|
  t.pattern = 'test/unit/test_*.rb'
end

Rake::TestTask.new :multiprocess_test do |t|
  t.pattern = 'test/test_api_multiprocess.rb'
end

Rake::TestTask.new :integration do |t|
  t.pattern = 'test/integration/test_*.rb'
end

task :start_servers do
  pids = []
  (8001..8009).each do |port|
    puts "Starting on port #{port}..."
    pids << spawn({'HOST_ID' => port, 'RUN_ID' => '1'}, "#{BASEDIR}/bin/rackup -p #{port} -s Puma")
  end
  puts 'Now wait for Pumas to start...'
  begin
    pids.each { |p| Process.wait p }
  ensure
    pids.each do |p|
      begin
        puts "Killing #{p}..."
        Process.kill 'INT', p
      rescue
        # ignored
      end
    end
  end
end

task :create_schema do
  client = Cql::Client.connect(host: 'localhost')
  begin
    client.execute 'DROP KEYSPACE counters'
  rescue
    # ignored
  end
  client.execute <<-EOF
    CREATE KEYSPACE counters WITH
      replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
  EOF
  require 'bootstrap'
  require 'aggregate_table'

  %w(approx_distinct sum min max).each do |table|
    AggregateTable.new(table, nil).create_table
  end
end
