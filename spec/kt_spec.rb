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
  end
end
