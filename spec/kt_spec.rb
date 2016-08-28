# encoding: UTF-8

require "spec_helper"

describe KT do
  before :all do
    @kt = KT.new(host: HOST, port: PORT, poolsize: 5, timeout: 5.0)
  end

  after :all do
    @kt.clear
  end

  describe "count" do
    it "returns 0 by default" do
      expect(@kt.count).to eql(0)
    end

    it "returns 2 after some keys were inserted" do
      @kt.set("japan", "tokyo")
      @kt.set("china", "beijing")

      expect(@kt.count).to eql(2)
    end
  end

  describe "get/set/remove" do
    it "sets a few keys then it gets them" do
      ["a", "b", "c"].each do |k|
        @kt.set(k, k + "aaa")
        expect(@kt.get(k)).to eql(k + "aaa")
      end
    end

    it "removes a key" do
      @kt.set("to/be/removed", "42")
      @kt.remove("to/be/removed")
      expect(@kt.get("to/be/removed")).to eql(nil)
    end

    it "get returns nil if not found" do
      expect(@kt.get("not/existing")).to eql(nil)
    end

    describe "get!" do
      it "returns a string if existing" do
        @kt.set("foo", "bar")
        expect(@kt.get("foo")).to eql("bar")
      end

      it "raises error if not found" do
        expect {
          @kt.get!("not/existing")
        }.to raise_error(KT::RecordNotFound)
      end
    end

    describe "remove" do
      it "returns true if key was deleted" do
        @kt.set("foo", "bar")
        expect(@kt.remove("foo")).to eql(true)
      end

      it "returns false if key was not found" do
        expect(@kt.remove("not/existing")).to eql(false)
      end
    end

    describe "remove!" do
      it "returns nothing if key was deleted" do
        @kt.set("foo", "bar")
        @kt.remove("foo")
        expect(@kt.get("foo")).to eql(nil)
      end

      it "raises error if not found" do
        expect {
          @kt.remove!("not/existing")
        }.to raise_error(KT::RecordNotFound)
      end
    end

    describe "set" do
      it "accepts expire" do
        @kt.set("expirekey", "expiredvalue", expire: 1)
        expect(@kt.get("expirekey")).to eql("expiredvalue")
        @kt.vacuum # trigger garbage collection

        expect {
          @kt.get("expirekey")
        }.to eventually(eq(nil)).within(5)
      end
    end
  end

  describe "fetch" do
    it "fetches block if not found in cache" do
      block_ran = false
      user = @kt.fetch("my.key") do
        block_ran = true
        User.new(
          :id => 1,
          :name => "Aaa bbb",
          :email => "foo@example.com",
          :age => 20
        )
      end

      expect(block_ran).to eql(true)
      expect(user.id).to eql(1)
      expect(user.name).to eql("Aaa bbb")
      expect(user.email).to eql("foo@example.com")
      expect(user.age).to eql(20)
    end

    it "fetches from cache if already in cache" do
      block_ran = false
      user1 = @kt.fetch("my.key2") do
        block_ran = true
        User.new(
          :id => 1,
          :name => "Aaa bbb",
          :email => "foo@example.com",
          :age => 20
        )
      end
      expect(block_ran).to eql(true)

      block_ran = false

      user2 = @kt.fetch("my.key2") do
        block_ran = true
        User.new(
          :id => 1,
          :name => "Aaa bbb",
          :email => "foo@example.com",
          :age => 20
        )
      end

      expect(block_ran).to eql(false)
      expect(user1).to eql(user2)
    end

    it "sets cache if not found in cache" do
      block_ran = false
      user1 = @kt.fetch("my.key3") do
        block_ran = true
        User.new(
          :id => 2,
          :name => "Aaa ccc",
          :email => "foo2@example.com",
          :age => 30
        )
      end
      expect(block_ran).to eql(true)

      obj = @kt.get("my.key3")
      user = Marshal::load(obj)
      expect(user.id).to eql(2)
      expect(user.name).to eql("Aaa ccc")
      expect(user.email).to eql("foo2@example.com")
      expect(user.age).to eql(30)

      expect(user).to eql(user1)
    end

    it "does not set cache if block returns nil" do
      block_ran = false
      user1 = @kt.fetch("my.key4") do
        block_ran = true
        nil
      end
      expect(block_ran).to eql(true)

      obj = @kt.get("my.key4")
      expect(obj).to eql(nil)
    end
  end

  describe "bulk" do
    it "returns nil hash for not found keys" do
      expect(@kt.get_bulk(["foo1", "foo2", "foo3"])).to eql({})
    end

    it "returns hash with key value" do
      expected = {
        "cache/news/1" => "1",
        "cache/news/2" => "2",
        "cache/news/3" => "3",
        "cache/news/4" => "4",
        "cache/news/5" => "5",
        "cache/news/6" => "6"
      }
      expected.each do |k, v|
        @kt.set(k, v)
      end

      expect(@kt.get_bulk(expected.keys)).to eql(expected)
    end

    it "returns hash with found elements" do
      @kt.set("foo4", "4")
      @kt.set("foo5", "5")

      expect(@kt.get_bulk(["foo4", "foo5", "foo6"])).to eql({"foo4" => "4", "foo5" => "5"})
    end

    it "set_bulk sets multiple keys" do
      @kt.set_bulk({"foo7" => "7", "foo8" => "8", "foo9" => "9"})
      expect(@kt.get_bulk(["foo7", "foo8", "foo9"])).to eql({"foo7" => "7", "foo8" => "8", "foo9" => "9"})
    end

    it "remove_bulk deletes bulk items" do
      @kt.set_bulk({"foo7" => "7", "foo8" => "8", "foo9" => "9"})
      @kt.remove_bulk(["foo7", "foo8", "foo9"])
      expect(@kt.get_bulk(["foo7", "foo8", "foo9"])).to eql({})
    end

    it "returns the number of keys deleted" do
      @kt.set_bulk({"foo7" => "7", "foo8" => "8", "foo9" => "9"})
      expect(@kt.remove_bulk(["foo7", "foo8", "foo9", "foo1000"])).to eql(3)
    end
  end

  describe "match_prefix" do
    it "returns nothing for not found prefix" do
      expect(@kt.match_prefix("user:", 100)).to eql([])
    end

    it "returns correct results sorted" do
      @kt.set_bulk({"user:1" => "1", "user:2" => "2", "user:4" => "4"})
      @kt.set_bulk({"user:3" => "3", "user:5" => "5"})
      @kt.set_bulk({"usera" => "aaa", "users:bbb" => "bbb"})

      expect(@kt.match_prefix("user:")).to eql(["user:1", "user:2", "user:3", "user:4", "user:5"])
      # It returns the results in random order
      expect(@kt.match_prefix("user:", 2).size).to eql(2)
    end
  end

  describe "clear" do
    it "clears the database" do
      expect(@kt.count).to_not eql(0)
      @kt.clear
      expect(@kt.count).to eql(0)
    end
  end

  describe "cas" do
    describe "with old and new" do
      it "sets new value if old value is correct and returns true" do
        @kt.set("cas:1", "1")
        expect(@kt.cas("cas:1", "1", "2")).to eql(true)
        expect(@kt.get("cas:1")).to eql("2")
      end

      it "returns false if old value is not equal" do
        @kt.set("cas:2", "3")
        expect(@kt.cas("cas:2", "1", "2")).to eql(false)
        expect(@kt.get("cas:2")).to eql("3")
      end
    end

    describe "without old value" do
      it "sets the value if no record exists in db and returns true" do
        expect(@kt.cas("cas:3", nil, "5")).to eql(true)
        expect(@kt.get("cas:3")).to eql("5")
      end

      it "returns false if record exists in db" do
        @kt.set("cas:4", "2")
        expect(@kt.cas("cas:4", nil, "5")).to eql(false)
        expect(@kt.get("cas:4")).to eql("2")
      end
    end

    describe "without new value" do
      it "removes record if it exists in db and returns true" do
        @kt.set("cas:5", "1")
        expect(@kt.cas("cas:5", "1", nil)).to eql(true)
        expect(@kt.get("cas:5")).to eql(nil)
      end

      it "returns false if no record exists in db" do
        expect(@kt.cas("cas:6", "1", nil)).to eql(false)
        expect(@kt.get("cas:6")).to eql(nil)
      end
    end
  end

  describe "ttl" do
    it "returns -2 if key does not exist" do
      expect(@kt.ttl("ttl/not/existing/key")).to eql(-2)
    end

    it "returns -1 if key exists but no ttl is set" do
      @kt.set("ttl/no/expire/key", "1")
      expect(@kt.ttl("ttl/no/expire/key")).to eql(-1)
    end

    it "returns ttl if key exists and it has ttl set" do
      @kt.set("ttl/test/key/1", "1", expire: 3)
      expect(@kt.ttl("ttl/test/key/1")).to eql(2)
    end

    it "returns ttl if key exists and it has ttl set to 5" do
      @kt.set("ttl/test/key/2", "1", expire: 5)
      expect(@kt.ttl("ttl/test/key/2")).to eql(4)
    end
  end

  describe "pttl" do
    it "returns -2 if key does not exist" do
      expect(@kt.pttl("ttl/not/existing/key")).to eql(-2)
    end

    it "returns -1 if key exists but no ttl is set" do
      @kt.set("ttl/no/expire/key", "1")
      expect(@kt.pttl("ttl/no/expire/key")).to eql(-1)
    end

    it "returns ttl if key exists and it has ttl set" do
      @kt.set("ttl/test/key/1", "1", expire: 3)
      ttl = @kt.pttl("ttl/test/key/1")
      expect(ttl < 3).to be_truthy
      expect(ttl > 2).to be_truthy
    end

    it "returns ttl if key exists and it has ttl set to 5" do
      @kt.set("ttl/test/key/2", "1", expire: 5)
      ttl = @kt.pttl("ttl/test/key/2")
      expect(ttl < 5).to be_truthy
      expect(ttl > 4).to be_truthy
    end
  end

  describe "binary" do
    it "sets binary and gets it" do
      @kt.set_bulk({"Café" => "foo"})
      expect(@kt.get("Café")).to eql("foo")

      @kt.set_bulk({"foo" => "Café"})
      expect(@kt.get_bulk(["foo"])).to eql({"foo" => "Café"})
    end

    it "sets string using newlines and gets it" do
      @kt.set_bulk({"foo" => "my\n\ttest"})
      expect(@kt.get_bulk(["foo"])).to eql({"foo" => "my\n\ttest"})
    end
  end
end
