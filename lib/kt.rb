require "excon"
require "connection_pool"
require "base64"
require "kt/errors"
require "kt/kv"
require "kt/version"

class KT
  IDENTITY_ENCODING = "text/tab-separated-values"
  BASE64_ENCODING   = "text/tab-separated-values; colenc=B"

  IDENTITY_HEADERS = {"Content-Type" => IDENTITY_ENCODING}
  BASE64_HEADERS   = {"Content-Type" => BASE64_ENCODING}
  EMPTY_HEADERS    = {}

  def initialize(options)
    @host = options.fetch(:host, "127.0.0.1")
    @port = options.fetch(:port, 1978)
    @poolsize = options.fetch(:poolsize, 5)
    @timeout = options.fetch(:timeout, 5.0)

    @pool = ConnectionPool.new(size: @poolsize, timeout: @timeout) do
      Excon.new("http://#{@host}:#{@port}")
    end
  end

  # count returns the number of records in the database
  def count
    status, m = do_rpc("/rpc/status")

    if status != 200
      raise_error(m)
    end

    find_rec(m, "count").value.to_i
  end

  # clear removes all records in the database
  def clear
    status, m = do_rpc("/rpc/clear")

    if status != 200
      raise_error(m)
    end
  end

  # vacuum triggers garbage collection of expired records
  def vacuum
    status, m = do_rpc("/rpc/vacuum")

    if status != 200
      raise_error(m)
    end
  end

  # get retrieves the data stored at key.
  # It returns nil if no such data is found
  def get(key)
    status, body = do_rest("GET", key, nil)

    case status
    when 200
      body
    when 404
      nil
    end
  end

  # get! retrieves the data stored at key.
  # KT::RecordNotFound is raised if not such data is found
  def get!(key)
    value = get(key)
    if value != nil
      value
    else
      raise KT::RecordNotFound.new("Key: #{key} not found")
    end
  end

  # get_bulk retrieves the keys in the list
  # It returns a hash of key => value.
  # If a key was not found in the database, the value in return hash will be nil
  def get_bulk(keys)
    req = keys.map do |key|
      KT::KV.new("_#{key}", "")
    end

    status, res_body = do_rpc("/rpc/get_bulk", req)

    if status != 200
      raise_error(res_body)
    end

    res = {}

    res_body.each do |kv|
      if kv.key.start_with?('_')
        res[kv.key[1, kv.key.size - 1]] = kv.value
      end
    end

    return res
  end

  # fetch retrives the keys from cache
  # if key is found it returns the unmarshaled value
  # if key is not found it runs the block sends the value and returns it
  def fetch(key, &block)
    value = get(key)
    if value
      Marshal::load(value)
    else
      block.call.tap do |value|
        set(key, Marshal::dump(value))
      end
    end
  end

  # ttl returns the time to live for a key in seconds
  # if key does not exist, it returns -2
  # if key exists but no ttl is set, it returns -1
  # if key exists and it has a ttl, it returns the number of seconds remaining
  def ttl(key)
    req = [
      KT::KV.new("key", key),
    ]

    status, res_body = do_rpc("/rpc/check", req)

    case status
    when 200
      xt_pos = res_body.index{|kv| kv.key == "xt"}
      if xt_pos != nil
        expire_time = res_body[xt_pos].value.to_f
        [(expire_time - Time.now.to_f).to_i, 0].max
      else
        -1
      end
    when 450
      -2
    else
      raise_error(res_body)
    end
  end

  # pttl returns the time to live for a key in milliseconds
  # if key does not exist, it returns -2
  # if key exists but no ttl is set, it returns -1
  # if key exists and it has a ttl, it returns the number of milliseconds remaining
  def pttl(key)
    req = [
      KT::KV.new("key", key),
    ]

    status, res_body = do_rpc("/rpc/check", req)

    case status
    when 200
      xt_pos = res_body.index{|kv| kv.key == "xt"}
      if xt_pos != nil
        expire_time = res_body[xt_pos].value.to_f
        [expire_time - Time.now.to_f, 0].max
      else
        -1
      end
    when 450
      -2
    else
      raise_error(res_body)
    end
  end

  # set stores the data at key
  def set(key, value, expire: nil)
    req = [
      KT::KV.new("key", key),
      KT::KV.new("value", value),
    ]

    if expire
      req << KT::KV.new("xt", expire.to_s)
    end

    status, body = do_rpc("/rpc/set", req)

    if status != 200
      raise_error(body)
    end
  end

  # set_bulk sets multiple keys to multiple values
  def set_bulk(values)
    req = values.map do |key, value|
      KT::KV.new("_#{key}", value)
    end

    status, body = do_rpc("/rpc/set_bulk", req)

    if status != 200
      raise_error(body)
    end

    find_rec(body, "num").value.to_i
  end

  # remove deletes the data at key in the database.
  def remove(key)
    status, body = do_rest("DELETE", key, nil)

    if status == 404
      return false
    end

    if status != 204
      raise KT::Error.new(body)
    end

    return true
  end

  # remove! deletes the data at key in the database
  # it raises KT::RecordNotFound if key was not found
  def remove!(key)
    unless remove(key)
      raise KT::RecordNotFound.new("key #{key} was not found")
    end
  end

  # remove_bulk deletes multiple keys.
  # it returnes the number of keys deleted
  def remove_bulk(keys)
    req = keys.map do |key|
      KV.new("_#{key}", "")
    end

    status, body = do_rpc("/rpc/remove_bulk", req)

    if status != 200
      raise_error(body)
    end

    find_rec(body, "num").value.to_i
  end

  # match_prefix performs the match_prefix operation against the server
  # It returns a sorted list of keys.
  # max_records defines the number of results to be returned
  # if negative, it means unlimited
  def match_prefix(prefix, max_records = -1)
    req = [
      KT::KV.new("prefix", prefix),
      KT::KV.new("max", max_records.to_s)
    ]

    status, body = do_rpc("/rpc/match_prefix", req)

    if status != 200
      raise_error(body)
    end

    res = []

    body.each do |kv|
      if kv.key.start_with?('_')
        res << kv.key[1, kv.key.size - 1]
      end
    end

    return res
  end

  # cas executes a compare and swap operation
  # if both old and new provided it sets to new value if previous value is old value
  # if no old value provided it will set to new value if key is not present in db
  # if no new value provided it will remove the record if it exists
  # it returns true if it succeded or false otherwise
  def cas(key, oval = nil, nval = nil)
    req = [KT::KV.new("key", key)]
    if oval != nil
      req << KT::KV.new("oval", oval)
    end
    if nval != nil
      req << KT::KV.new("nval", nval)
    end

    status, body = do_rpc("/rpc/cas", req)

    if status == 450
      return false
    end

    if status != 200
      raise_error(body)
    end

    return true
  end

  # cas! works the same as cas but it raises error on failure
  def cas!(key, oval = nil, nval = nil)
    if !cas(key, oval, nval)
      raise KT::CASFailed.new("Failed compare and swap for #{key}")
    end
  end

  private

  def do_rpc(path, values=nil)
    body, encoding = encode_values(values)
    headers = {"Content-Type" => encoding}

    @pool.with do |conn|
      res = conn.post(:path => path, :headers => headers, :body => body)
      # return res.status_code, decode_values(res.body, res.headers.get("Content-Type").join("; "))
      return res.status, decode_values(res.body, res.headers["Content-Type"])
    end
  end

  def do_rest(method, key, value="")
    @pool.with do |conn|
      res = conn.request(:method => method, :path => url_encode(key), :headers => EMPTY_HEADERS, :body => value)
      return res.status, res.body
    end
  end

  def find_rec(kv_list, key)
    kv_list.each do |kv|
      if kv.key == key
        return kv
      end
    end

    KV.new("", "")
  end

  def raise_error(body)
    kv = find_rec(body, "ERROR")
    if kv == ""
      raise KT::Error.new("unknown error")
    end

    raise KT::Error.new("#{kv.value}")
  end

  def decode_values(body, content_type)
    # Ideally, we should parse the mime media type here,
    # but this is an expensive operation because mime is just
    # that awful.
    #
    # KT responses are pretty simple and we can rely
    # on it putting the parameter of colenc=[BU] at
    # the end of the string. Just look for B, U or s
    # (last character of tab-separated-values)
    # to figure out which field encoding is used.

    case content_type.chars.last
    when 'B'
      # base64 decode
      method = :base64_decode
    when 'U'
      # url decode
      method = :url_decode
    when 's'
      # identity decode
      method = :identity_decode
    else
      raise "kt responded with unknown content-type: #{content_type}"
    end

    # Because of the encoding, we can tell how many records there
    # are by scanning through the input and counting the \n's
    kv = body.each_line.map do |line|
      key, value = line.chomp.split("\t")
      KT::KV.new(send(method, key), send(method, value))
    end.to_a

    kv.to_a
  end

  def encode_values(kv_list)
    if kv_list.nil?
      return "", IDENTITY_ENCODING
    end

    has_binary = kv_list.any? do |kv|
      has_binary?(kv.key) || has_binary?(kv.value)
    end

    str = StringIO.new

    kv_list.each do |kv|
      if has_binary
        str << Base64.strict_encode64(kv.key)
        str << "\t"
        str << Base64.strict_encode64(kv.value)
      else
        str << kv.key
        str << "\t"
        str << kv.value
      end
      str << "\n"
    end

    encoding = has_binary ? BASE64_ENCODING : IDENTITY_ENCODING

    return str.string, encoding
  end

  def identity_decode(value)
    value.force_encoding("utf-8")
  end

  def base64_decode(value)
    Base64.strict_decode64(value).force_encoding("utf-8")
  end

  def url_decode(value)
    URI.unescape(value).force_encoding("utf-8")
  end

  def url_encode(key)
    "/" + URI.escape(key).gsub("/", "%2F")
  end

  def has_binary?(value)
    value.bytes.any? do |c|
      c < 0x20 || c > 0x7e
    end
  end
end
