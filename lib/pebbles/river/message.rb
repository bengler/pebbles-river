module Pebbles
  module River

    class InvalidPayloadError < StandardError

      attr_reader :payload

      def initalize(message, payload)
        super(message)
        @payload = payload
      end

    end

    class Message

      attr_reader :payload
      attr_reader :queue

      def self.deserialize_payload(payload)
        if payload
          begin
            return JSON.parse(payload)
          rescue => e
            raise InvalidPayloadError.new(e.message, payload)
          end
        end
      end

      def initialize(raw_message, queue = nil)
        @queue = queue
        @raw_message = raw_message
        @payload = self.class.deserialize_payload(raw_message[:payload])
      end

      def ==(other)
        other &&
          other.is_a?(Message) &&
          other.payload == @payload
      end

      def delivery_tag
        @raw_message[:delivery_details][:delivery_tag]
      end

      def ack
        @queue.ack(delivery_tag: delivery_tag)
      end

      def nack
        @queue.nack(delivery_tag: delivery_tag)
      end

    end

  end
end