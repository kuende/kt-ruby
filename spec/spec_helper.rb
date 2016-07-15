require "rubygems"
require "bundler/setup"

require "kt"
require "pry"

HOST = "127.0.0.1"
PORT = 1978

RSpec.configure do |config|
  config.filter_run focus: true
  config.run_all_when_everything_filtered = true
end
