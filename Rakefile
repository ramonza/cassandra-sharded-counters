require 'rake/testtask'
require_relative 'bootstrap'
require 'cql'

BASEDIR = File.dirname(__FILE__)

Rake::TestTask.new do |t|
  t.pattern = 'test/unit/test_*.rb'
end

Rake::TestTask.new :multiprocess_test do |t|
  t.pattern = 'test/integration/test_*.rb'
end

Rake::TestTask.new :rack_test do |t|
  t.pattern = 'test/rack/test_*.rb'
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
      replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
  EOF

  client.use('counters')

  # current run
  client.execute <<-EOF
    CREATE TABLE host_current_runs (
      host_id VARINT,
      aggregate_table VARCHAR,
      current_run VARINT,
      PRIMARY KEY (host_id, aggregate_table)
    )
  EOF

  # aggregate tables
  %w(approx_distinct sum min max).each do |table|
    client.execute <<-EOF
      CREATE TABLE #{table} (
        row_key VARCHAR,
        column_key VARCHAR,
        host_id VARINT,
        run VARINT,
        generation VARINT,
        counter_state BLOB,
        PRIMARY KEY (row_key, column_key, host_id, run, generation)
      )
    EOF
  end


end
