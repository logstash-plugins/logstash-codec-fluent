# encoding: utf-8
require "logstash/codecs/base"
require "logstash/event"
require "logstash/timestamp"
require "logstash/util"

require 'logstash/plugin_mixins/event_support/event_factory_adapter'
require 'logstash/plugin_mixins/validator_support/field_reference_validation_adapter'

# This codec handles fluentd's msgpack schema.
#
# For example, you can receive logs from `fluent-logger-ruby` with:
# [source,ruby]
#     input {
#       tcp {
#         codec => fluent {
#           nanosecond_precision => true
#         }
#         port => 4000
#       }
#     }
#
# And from your ruby code in your own application:
# [source,ruby]
#     logger = Fluent::Logger::FluentLogger.new(nil, :host => "example.log", :port => 4000)
#     logger.post("some_tag", { "your" => "data", "here" => "yay!" })
#
# Notes:
#
# * to handle EventTime msgpack extension, you must specify nanosecond_precision parameter as true.
#
class LogStash::Codecs::Fluent < LogStash::Codecs::Base
  require "logstash/codecs/fluent/event_time"

  extend LogStash::PluginMixins::ValidatorSupport::FieldReferenceValidationAdapter

  include LogStash::PluginMixins::EventSupport::EventFactoryAdapter

  config_name "fluent"

  config :nanosecond_precision, :validate => :boolean, :default => false

  # Defines a target field for placing decoded fields.
  # If this setting is omitted, data gets stored at the root (top level) of the event.
  #
  # NOTE: the target is only relevant while decoding data into a new event.
  config :target, :validate => :field_reference

  def register
    require "msgpack"
    @factory = MessagePack::Factory.new
    if @nanosecond_precision
      @factory.register_type(EventTime::TYPE, EventTime)
    end
    @packer = @factory.packer
    @decoder = @factory.unpacker
  end

  def decode(data, &block)
    @decoder.feed_each(data) do |item|
      decode_event(item, &block)
    end
  end # def decode

  def encode(event)
    # Ensure tag to "tag1.tag2.tag3" style string.
    # Fluentd cannot handle Array class value in forward protocol's tag.
    tag = forwardable_tag(event)
    epochtime = if @nanosecond_precision
                  EventTime.new(event.timestamp.to_i, event.timestamp.usec * 1000)
                else
                  event.timestamp.to_i
                end

    # use normalize to make sure returned Hash is pure Ruby for
    # MessagePack#pack which relies on pure Ruby object recognition
    data = LogStash::Util.normalize(event.to_hash)
    # timestamp is serialized as a iso8601 string
    # merge to avoid modifying data which could have side effects if multiple outputs
    @packer.clear
    @on_event.call(event, @packer.pack([tag, epochtime, data.merge(LogStash::Event::TIMESTAMP => event.timestamp.to_iso8601)]))
  end # def encode

  def forwardable_tag(event)
    tag = event.get("tags") || "log"
    case tag
    when Array
      tag.join('.')
    when String
      tag
    else
      tag.to_s
    end
  end

  private

  def decode_fluent_time(fluent_time)
    case fluent_time
    when Integer
      fluent_time
    when EventTime
      Time.at(fluent_time.sec, fluent_time.nsec / 1000.0)
    end
  end

  def decode_event(data, &block)
    tag = data[0]
    entries = data[1]

    case entries
    when String
      # PackedForward
      option = data[2]
      compressed = (option && option['compressed'] == 'gzip')
      if compressed
        raise(LogStash::Error, "PackedForward with compression is not supported")
      end

      entries_decoder = @factory.unpacker
      entries_decoder.feed_each(entries) do |entry|
        yield generate_event(entry[1], entry[0], tag)
      end
    when Array
      # Forward
      entries.each do |entry|
        yield generate_event(entry[1], entry[0], tag)
      end
    when Integer, EventTime
      # Message
      yield generate_event(data[2], entries, tag)
    else
      raise(LogStash::Error, "Unknown event type")
    end
  rescue StandardError => e
    @logger.error("Fluent parse error, original data now in message field", :error => e, :data => data)
    yield event_factory.new_event("message" => data, "tags" => [ "_fluentparsefailure" ])
  end

  def generate_event(map, fluent_time, tag)
    epoch_time = decode_fluent_time(fluent_time)
    event = targeted_event_factory.new_event(map)
    event.set(LogStash::Event::TIMESTAMP, LogStash::Timestamp.at(epoch_time))
    event.tag(tag)
    event
  end

end # class LogStash::Codecs::Fluent
