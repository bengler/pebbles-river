require 'spec_helper'

include Pebbles::River

describe Worker do

  subject do
    Worker
  end

  let :invalid_handler do
    Class.new.new
  end

  let :null_handler do
    handler = double('null_handler')
    handler.stub(:call) { }
    handler
  end

  let :false_handler do
    handler = double('null_handler')
    handler.stub(:call) { false }
    handler
  end

  let :io_error do
    IOError.new("This is not the exception you are looking for")
  end

  let :io_error_raising_handler do
    handler = double('io_error_raising_handler')
    handler.stub(:call).and_raise(io_error)
    handler
  end

  let :connection_exception do
    Bunny::ConnectionError.new("Fail!")
  end

  let :payload do
    {'answer' => 42}
  end

  let :raw_message do
    {
      header: 'someheader',
      payload: JSON.dump(payload),
      delivery_details: {delivery_tag: 'foo'}
    }
  end  

  let :queue do
    queue = double('Bunny::Queue')
    queue.stub(:close) { nil }
    queue.stub(:pop) { |&block|
      block.call(raw_message)
    }
    queue.stub(:ack) { }
    queue.stub(:nack) { }
    queue
  end

  let :message do
    Message.new(raw_message, queue)
  end

  let :river do
    river = double('Pebbles::River::River')
    river.stub(:connected?) { true }
    river.stub(:queue).and_return(queue)
    river.stub(:connect).and_return(nil)
    river
  end

  before do
    River.stub(:new).and_return(river)
  end

  describe '#initialize' do
    it 'accepts a handler' do
      worker = subject.new(null_handler)
      expect(worker.handler).to eq null_handler
    end

    it 'requires that handler implement #call' do
      expect(-> {
        subject.new(invalid_handler)
      }).to raise_error(ArgumentError)
    end

    it 'accepts queue options' do
      worker = subject.new(null_handler, queue: {event: 'foo'})
      expect(worker.queue_options).to eq({event: 'foo'})
    end
  end

  describe '#run_once' do

    it 'creates a queue and runs worker with it' do
      expect(queue).to receive(:pop).at_least(1).times
      expect(queue).to receive(:ack).at_least(1).times
      expect(queue).to_not receive(:nack)

      expect(null_handler).to receive(:call).with(message)

      expect(river).to receive(:connected?).with(no_args).at_least(1).times
      expect(river).to_not receive(:connect)
      expect(river).to receive(:queue).with({name: 'foo'})

      subject.new(null_handler, queue: {name: 'foo'}).run_once
    end

    context 'when queue is empty' do
      it 'does nothing' do
        queue.stub(:pop) { |&block|
          block.call({payload: :queue_empty, delivery_details: {}})
        }

        expect(queue).to receive(:pop).at_least(1).times
        expect(queue).to_not receive(:ack)
        expect(queue).to_not receive(:nack)

        expect(null_handler).not_to receive(:call)

        subject.new(null_handler, queue: {name: 'foo'}).run_once
      end

      it 'calls #on_idle if implemented' do
        queue.stub(:pop) { |&block|
          block.call({payload: :queue_empty, delivery_details: {}})
        }

        null_handler.stub(:on_idle) { }
        expect(null_handler).to receive(:on_idle)

        subject.new(null_handler, queue: {name: 'foo'}).run_once
      end
    end

    context 'when handler is successful' do
      it 'acks the message' do
        expect(queue).to receive(:ack).at_least(1).times
        expect(queue).to_not receive(:nack)
        expect(queue).to_not receive(:close)

        expect(river).to receive(:connected?).with(no_args).at_least(1).times
        expect(river).to_not receive(:connect)

        subject.new(null_handler, queue: {name: 'foo'}).run_once
      end
    end

    context 'when handler returns false' do
      it 'nacks the message' do
        expect(queue).to receive(:nack).at_least(1).times
        expect(queue).to_not receive(:ack)
        expect(queue).to_not receive(:close)

        expect(river).to receive(:connected?).with(no_args).at_least(1).times
        expect(river).to_not receive(:connect)

        subject.new(false_handler, queue: {name: 'foo'}).run_once
      end
    end

    context 'when handler throws exception' do

      let :on_exception_callback do
        on_exception_callback = double('on_exception')
        on_exception_callback.stub(:call) { }
        on_exception_callback
      end

      it 'nacks the message' do
        expect(queue).to receive(:nack).at_least(1).times
        expect(queue).to_not receive(:close)

        subject.new(io_error_raising_handler, queue: {name: 'foo'}).run_once
      end

      [
        Bunny::ConnectionError,
        Bunny::ForcedChannelCloseError,
        Bunny::ForcedConnectionCloseError,
        Bunny::ServerDownError,
        Bunny::ProtocolError,
        Errno::ECONNRESET
      ].each do |exception_class|
        context "connection exception #{exception_class}" do
          let :exception do
            exception_class.new("Dangit")
          end

          let :handler do
            handler = double('handler')
            handler.stub(:call).and_return {
              raise exception
            }
            handler
          end

          it "performs connection reset on #{exception_class}" do
            expect(queue).to receive(:close).at_least(1).times

            expect(handler).to receive(:call).with(message)

            expect(river).to receive(:connected?).with(no_args).at_least(1).times
            expect(river).to_not receive(:connect)
            expect(river).to receive(:queue).with({name: 'foo'})
            expect(river).to receive(:disconnect).at_least(1).times

            subject.new(handler, queue: {name: 'foo'}).run_once
          end

          it "does not call #on_exception on connection error" do
            on_exception_callback = double('on_connection_error')
            on_exception_callback.stub(:call) { }
            expect(on_exception_callback).to_not receive(:call)

            expect(queue).to receive(:close).at_least(1).times

            expect(handler).to receive(:call).at_least(1).times

            expect(river).to receive(:connected?).with(no_args).at_least(1).times
            expect(river).to_not receive(:connect)
            expect(river).to receive(:disconnect).at_least(1).times

            subject.new(handler,
              queue: {name: 'foo'},
              on_exception: on_exception_callback).run_once
          end

          it "logs error to logger" do
            expect(handler).to receive(:call).with(message)

            logger = double('logger')
            logger.stub(:error) { }
            expect(logger).to receive(:error).
              with(/#{Regexp.escape(exception.message)}/).at_least(1).times

            river.stub(:disconnect)

            subject.new(handler,
              queue: {name: 'foo'},
              logger: logger).run_once
          end
        end
      end

      context 'non-connection exception' do
        it "calls #on_exception" do
          expect(queue).to_not receive(:close)
          expect(on_exception_callback).to receive(:call).with(io_error)

          subject.new(io_error_raising_handler,
            queue: {name: 'foo'},
            on_exception: on_exception_callback).run_once
        end

        it "logs error to logger" do
          on_exception_callback.stub(:call)

          river.stub(:disconnect)

          logger = double('logger')
          logger.stub(:error) { }
          expect(logger).to receive(:error).
            with(/#{Regexp.escape(io_error.message)}/).at_least(1).times

          subject.new(io_error_raising_handler,
            queue: {name: 'foo'},
            logger: logger).run_once
        end
      end

    end

  end

  describe '#run' do
    it 'runs indefinitely until #enabled? returns false' do
      count = 0

      handler = double('handler')
      handler.stub(:call) { }

      expect(handler).to receive(:call).at_least(10).times

      worker = subject.new(handler, queue: {name: 'foo'})
      worker.stub(:enabled?) {
        count += 1
        count <= 10
      }
      worker.run
    end

    context 'when queue is empty' do
      it 'calls #sleep to delay polling a bit' do
        queue.stub(:pop) { |&block|
          block.call({payload: :queue_empty, delivery_details: {}})
        }

        count = 0
        worker = subject.new(null_handler, queue: {name: 'foo'})
        worker.stub(:sleep) { }
        expect(worker).to receive(:sleep).at_least(9).times
        worker.stub(:enabled?) {
          count += 1
          count <= 10
        }
        worker.run
      end
    end

    it 'continues on exception'
    it 'calls #should_run? on handler'
  end

  describe 'Worker.run' do
    it 'wraps Worker#run'
  end

end
