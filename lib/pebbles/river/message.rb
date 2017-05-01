module Pebbles
  module River

    class InvalidPayloadError < StandardError

      attr_reader :content

      def initialize(message, content)
        super("#{message}. Content was: #{content.inspect}")
        @content = content
      end

    end

    class Message

      attr_reader :payload
      attr_reader :queue
      attr_reader :delivery_info

      def self.deserialize_payload(content)
        if content
          begin
            return JSON.parse(content)
          rescue => e
            raise InvalidPayloadError.new(e.message, content)
          end
        end
      end

      def initialize(content, delivery_info, queue = nil)
        @queue = queue
        @delivery_info = delivery_info
        @payload = self.class.deserialize_payload(content)
        @replied = false
      end

      def ==(other)
        other &&
          other.is_a?(Message) &&
          other.payload == @payload
      end

      def delivery_tag
        @delivery_info.delivery_tag if @delivery_info
      end

      def ack
        if !@replied && (tag = delivery_tag)
          @queue.channel.ack(tag)
          @replied = true
        end
      end

      def nack
        if !@replied && (tag = delivery_tag)
          @queue.channel.nack(tag, false, true)
          @replied = true
        end
      end

    end

  end
end
