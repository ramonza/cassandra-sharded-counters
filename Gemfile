#ruby=jruby
#ruby-env-JRUBY_OPTS=--1.9 -J-XX:+TieredCompilation -J-Djruby.launch.inproc=true

source 'https://rubygems.org'

gem 'jbundler'
gem 'grape'
gem 'cql-rb'
gem 'puma'
gem 'pry'

group :test do
	gem 'minitest'
  gem 'minitest-reporters'
  gem 'rack-test'
end

group :development do
	gem 'rake'
  gem 'ruby-debug-base'
end
