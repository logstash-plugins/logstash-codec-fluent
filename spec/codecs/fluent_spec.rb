# encoding: utf-8
require_relative "../spec_helper"
require "logstash/plugin"
require "logstash/event"
require "msgpack"

describe LogStash::Codecs::Fluent do
  before do
    @factory = MessagePack::Factory.new
    @factory.register_type(LogStash::Codecs::Fluent::EventTime::TYPE,
                           LogStash::Codecs::Fluent::EventTime)
    @packer = @factory.packer
    @unpacker = @factory.unpacker
  end

  subject { LogStash::Codecs::Fluent.new(config) }

  let(:config) { Hash.new }

  let(:properties) { {:name => "foo" } }
  let(:event)      { LogStash::Event.new(properties) }
  let(:timestamp)  { event.timestamp }
  let(:epochtime)  { timestamp.to_i }
  let(:tag)  { "mytag" }
  let(:data) { { 'name' => 'foo', 'number' => 42 } }

  it "should register without errors" do
    plugin = LogStash::Plugin.lookup("codec", "fluent").new
    expect { plugin.register }.to_not raise_error
  end

  describe "event encoding" do

    it "should encode as message pack format" do
      subject.on_event do |event, data|
        @unpacker.feed_each(data) do |fields|
          expect(fields[0]).to eq("log")
          expect(fields[2]["name"]).to eq("foo")
        end
      end
      subject.encode(event)
    end

  end

  describe "event encoding with EventTime" do

    let(:config) { super().merge "nanosecond_precision" => true }

    it "should encode as message pack format" do
      subject.on_event do |event, data|
        @unpacker.feed_each(data) do |fields|
          expect(fields[0]).to eq("log")
          expect(fields[2]["name"]).to eq("foo")
        end
      end
      subject.encode(event)
    end

  end

  describe "event decoding" do

    let(:message) do
      @packer.pack([tag, epochtime, data.merge(LogStash::Event::TIMESTAMP => timestamp.to_iso8601)])
    end

    it "should decode without errors" do
      decoded = false
      subject.decode(message) do |event|
        expect(event.get("name")).to eq("foo")
        decoded = true
      end
      expect(decoded).to be true
    end

    it "should tag event" do
      subject.decode(message) do |event|
        expect(event.get("tags")).to eql [ tag ]
      end
    end

  end

  describe "event decoding with EventTime" do

    let(:epochtime) { LogStash::Codecs::Fluent::EventTime.new(timestamp.to_i,
                                                              timestamp.usec * 1000)  }
    let(:data)      { LogStash::Util.normalize('name' => 'foo', 'number' => 42) }
    let(:message) do
      @packer.pack([tag, epochtime, data.merge(LogStash::Event::TIMESTAMP => timestamp.to_iso8601)])
    end
    subject { LogStash::Plugin.lookup("codec", "fluent").new({"nanosecond_precision" => true}) }

    it "should decode without errors" do
      decoded = false
      subject.decode(message) do |event|
        expect(event.get("name")).to eq("foo")
        decoded = true
      end
      expect(decoded).to be true
    end

  end

  describe "event decoding with target" do

    let(:tag)       { "a_tag" }
    let(:epochtime) { 123 }
    let(:data)      { LogStash::Util.normalize('name' => 'foo') }
    let(:message) do
      @packer.pack([tag, epochtime, data])
    end

    let(:config) { super().merge "target" => '[bar]' }

    it "should decode without errors" do
      decoded = false
      subject.decode(message) do |event|
        expect(event.include?("name")).to be false
        expect(event.get("bar")).to eql('name' => "foo")
        decoded = true
      end
      expect(decoded).to be true
    end

    it "should tag event" do
      subject.decode(message) do |event|
        expect(event.get("tags")).to eql [ 'a_tag' ]
      end
    end

    it "should set timestamp" do
      subject.decode(message) do |event|
        expect(event.timestamp.to_i).to eql(epochtime)
      end
    end

  end

  describe "forward protocol tag" do

    describe "when passing Array value" do
      let(:properties) { {:tags => ["test", "logstash"], :name => "foo" } }

      it "should be joined with '.'" do
        subject.forwardable_tag(event) do |tag|
          expect(tag).to eq("test.logstash")
        end
      end
    end

    describe "when passing String value" do
      let(:properties) { {:tags => "test.logstash", :name => "foo" } }

      it "should be pass-through" do
        subject.forwardable_tag(event) do |tag|
          expect(tag).to eq("test.logstash")
        end
      end
    end

    describe "when passing other value" do
      let(:properties) { {:tags => :symbol, :name => "foo" } }

      it "should be called to_s" do
        subject.forwardable_tag(event) do |tag|
          expect(tag).to eq("symbol")
        end
      end
    end

  end

  describe "event decoding (buckets of events)" do

    let(:data) { LogStash::Util.normalize(event.to_hash) }
    let(:message) do
      @packer.pack([tag,
                    [
                      [epochtime, data.merge(LogStash::Event::TIMESTAMP => timestamp.to_iso8601)],
                      [epochtime, data.merge(LogStash::Event::TIMESTAMP => timestamp.to_iso8601)],
                      [epochtime, data.merge(LogStash::Event::TIMESTAMP => timestamp.to_iso8601)]
                    ]
                   ])
    end

    it "should decode without errors" do
      count = 0

      subject.decode(message) do |event|
        expect(event.get("name")).to eq("foo")
        count += 1
      end

      expect(count).to eq(3)
    end

  end

  describe "event decoding (broken package)" do

    let(:epochtime) { timestamp.to_s }
    let(:message) do
      MessagePack.pack([tag, epochtime, data])
    end

    it "should decode with errors" do
      subject.decode(message) do |event|
        expect(event.get("name")).not_to eq("foo")
      end
    end

    it "should inject a failure event" do
      subject.decode(message) do |event|
        expect(event.get("tags")).to include("_fluentparsefailure")
      end
    end

  end

end
