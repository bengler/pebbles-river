module Pebbles
  module River

    class ConnectionError < StandardError
      attr_reader :connection_exception

      def initialize(message, connection_exception = nil)
        super(message)
        @connection_exception = connection_exception
      end
    end

    class ConnectFailure < ConnectionError; end
    class SendFailure < ConnectionError; end

  end
end