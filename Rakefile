require 'rake/testtask'

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
		pids << spawn({'SHARD_ID' => "test-#{port}"}, "#{BASEDIR}/bin/rackup -p #{port} -s Puma")
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

