require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/fluent"
require "logstash/event"
require "insist"
require "msgpack"

describe LogStash::Codecs::Fluent do
  subject do
    next LogStash::Codecs::Fluent.new
  end

  context "#decode" do
    it "should return an event from the msgpack data" do
      data = ['foobar', Time.now.to_i,
              {"foo" => "bar", "baz" => {"bah" => ["a","b","c"]}}]

      subject.decode(MessagePack.pack(data)) do |event|
        insist { event.is_a? LogStash::Event }
        insist { event[LogStash::Event::TIMESTAMP] } == LogStash::Timestamp.at(data[1])
        insist { event["tags"] } == data[0]
        insist { event["foo"] } == data[2]["foo"]
        insist { event["baz"] } == data[2]["baz"]
        insist { event["bah"] } == data[2]["bah"]
      end
    end

    context "processing non fluent" do
      it "falls back to raw message" do
        decoded = false
        data = "something that isn't fluent"

        subject.decode(MessagePack.pack(data)) do |event|
          decoded = true
          insist { event.is_a?(LogStash::Event) }
          insist { event["message"] } == "\xBBsomething that isn't fluent"
          insist { event["tags"] }.include?("_fluentparsefailure")
        end
        insist { decoded } == true
      end
    end
  end
end
