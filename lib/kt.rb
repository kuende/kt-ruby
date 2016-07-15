require "excon"
require "connection_pool"
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
      KV.new("_#{key}", "")
    end

    status, res_body = do_rpc("/rpc/get_bulk", req)

    if status != 200
      raise_error(res_body)
    end

    res = {}

    res_body.each do |kv|
      if kv.key.starts_with?('_')
        res[kv.key[1, kv.key.size - 1]] = kv.value
      end
    end

    return res
  end

  # set stores the data at key
  def set(key, value)
    status, body = do_rest("PUT", key, value)

    if status != 201
      raise KT::Error.new(body)
    end
  end

  # set_bulk sets multiple keys to multiple values
  def set_bulk(values)
    req = values.map do |key, value|
      KV.new("_#{key}", value)
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

    has_binary = kv.any? do |kv|
      has_binary?(kv.key) || has_binary?(kv.value)
    end

    str = String.build do |str|
      kv.each do |kv|
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
    end

    encoding = has_binary ? BASE64_ENCODING : IDENTITY_ENCODING

    return str, encoding
  end

  def identity_decode(value)
    value
  end

  def base64_decode(value)
    Base64.strict_decode64(value)
  end

  def url_decode(value)
    URI.unescape(value)
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
