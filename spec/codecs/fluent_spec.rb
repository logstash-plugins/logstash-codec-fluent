# encoding: utf-8
require_relative "../spec_helper"
require "logstash/plugin"
require "logstash/event"

describe LogStash::Codecs::Fluent do

  let(:properties) { {:name => "foo" } }
  let(:event)      { LogStash::Event.new(properties) }

  it "should register without errors" do
    plugin = LogStash::Plugin.lookup("codec", "fluent").new
    expect { plugin.register }.to_not raise_error
  end

  describe "event encoding" do

    it "should encode as message pack format" do
      subject.on_event do |event, data|
        fields = MessagePack.unpack(data)
        expect(fields[0]).to eq("log")
        expect(fields[2]["name"]).to eq("foo")
      end
      subject.encode(event)
    end

  end

  describe "event decoding" do

    let(:tag)       { "mytag" }
    let(:epochtime) { event.timestamp.to_i }
    let(:data)      { LogStash::Util.normalize(event.to_hash) }
    let(:message) do
      MessagePack.pack([tag, epochtime, data.merge(LogStash::Event::TIMESTAMP => event.timestamp.to_iso8601)])
    end

    it "should decode without errors" do
      subject.decode(message) do |event|
        expect(event.get("name")).to eq("foo")
        expect(event.get("tags")).to eq("tag")
      end
    end

  end

  describe "event decoding (buckets of events)" do

    let(:tag)       { "mytag" }
    let(:epochtime) { event.timestamp.to_i }
    let(:data)      { LogStash::Util.normalize(event.to_hash) }
    let(:message) do
      MessagePack.pack([tag,
                        [
                          [epochtime, data.merge(LogStash::Event::TIMESTAMP => event.timestamp.to_iso8601)],
                          [epochtime, data.merge(LogStash::Event::TIMESTAMP => event.timestamp.to_iso8601)],
                          [epochtime, data.merge(LogStash::Event::TIMESTAMP => event.timestamp.to_iso8601)]
                        ]
                       ])
    end

    it "should decode without errors" do
      count = 0

      subject.decode(message) do |event|
        expect(event.get("name")).to eq("foo")
        expect(event.get("tags")).to eq("tag")
        count += 1
      end

      expect(count).to eq(3)
    end

  end

  describe "event decoding (broken package)" do

    let(:tag)       { "mytag" }
    let(:epochtime) { event.timestamp.to_s }
    let(:data)      { LogStash::Util.normalize(event.to_hash) }
    let(:message) do
      MessagePack.pack([tag,
                        epochtime, data.merge(LogStash::Event::TIMESTAMP => event.timestamp.to_iso8601)
                       ])
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
