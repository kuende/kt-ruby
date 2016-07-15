require "rubygems"
require "bundler/setup"

require "kt"
require "pry"
require "open3"

HOST = "127.0.0.1"
PORT = 1979

def start_server(host, port)
  if server_connected?(host, port)
    raise "Server already running on port #{port}"
  end

  args = ["ktserver", "-host", host, "-port", port.to_s]
  stdin, stdout, stderr, wait_thr = Open3.popen3(*args)

  50.times do
    if server_connected?(host, port)
      return wait_thr
    end
    sleep 0.05
  end

  raise "Server failed to start on port #{port}"
end

def stop_server(wait_thr)
  Process.kill("KILL", wait_thr.pid)
end

def server_connected?(host, port)
  begin
    socket = TCPSocket.new(host, port)
    # puts "Server connected"
    return true
  rescue => e
    # puts "Server failed: #{e}"
  ensure
    socket.close unless socket.nil?
  end

  false
end

RSpec.configure do |config|
  config.before(:suite) do
    @server_thr = start_server(HOST, PORT)
    unless @server_thr
      raise "Failed to connect to server"
    end
  end

  config.after(:suite) do
    if @server_thr
      stop_server(@server_thr)
    end
  end

  config.filter_run focus: true
  config.run_all_when_everything_filtered = true
end
