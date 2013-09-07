Bundler.require
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), 'lib'))
require 'logger'
$logger = Logger.new(STDOUT)
$logger.level = Logger::DEBUG