module LogStash; module Codecs; class Fluent;
  class EventTime
    attr_reader :sec, :nsec

    TYPE = 0

    def initialize(sec, nsec = 0)
      @sec = sec
      @nsec = nsec
    end

    def to_msgpack(io = nil)
      @sec.to_msgpack(io)
    end

    def to_msgpack_ext
      [@sec, @nsec].pack('NN')
    end

    def self.from_msgpack_ext(data)
      new(*data.unpack('NN'))
    end

    def to_json(*args)
      @sec
    end
  end
end; end; end
