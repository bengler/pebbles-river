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
      attr_reader :raw_message

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
        delivery_details[:delivery_tag]
      end

      def delivery_details
        @raw_message[:delivery_details] || {}
      end

      def ack
        @queue.ack(delivery_tag: delivery_tag)
      end

      def nack
        # TODO: This requires Bunny 0.9+. We therefore don't nack at all, but
        #   let messages simply expire, since pre-0.9 doesn't have a way to nack.
        #@channel.nack(delivery_tag, false)
      end

    end

  end
end