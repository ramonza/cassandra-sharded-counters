require 'rake/testtask'

Rake::TestTask.new do |t|
	t.pattern = 'test/unit/test_*.rb'
end

Rake::TestTask.new :integration do |t|
  t.pattern = 'test/integration/test_*.rb'
end

task :start_servers do
	pids = []
	(8001..8009).each do |port|
		puts "Starting on port #{port}..."
		pids << spawn({'SHARD_ID' => "test-#{port}"}, "rackup -p #{port} -s Puma")
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

