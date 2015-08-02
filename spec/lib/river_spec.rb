require 'spec_helper'

# Note to readers. This is verbose and ugly
# because I'm trying to understand what I'm doing.
# When I do understand it, I'll clean up the tests.
# Until then, please just bear with me.
# Or explain it to me :)
describe Pebbles::River::River do

  subject do
    Pebbles::River::River.new(environment: 'whatever')
  end

  CONNECTION_EXCEPTIONS = [
    Bunny::ConnectionError,
    Bunny::ConnectionClosedError,
    Bunny::ChannelAlreadyClosed,
    Bunny::ForcedChannelCloseError,
    Bunny::ForcedConnectionCloseError,
    Bunny::ServerDownError,
    Bunny::ProtocolError,
    Errno::ECONNRESET
  ]

  after(:each) do
    if (channel = subject.channel)
      channel.queues.each do |name, queue|
        queue.purge
        # If you don't delete the queue, the subscription will not
        # change, even if you give it a new one.
        queue.delete
      end
    end
    subject.disconnect
  end

  it { subject.should_not be_connected }

  it "gets the name right" do
    subject.connect
    subject.exchange.name.should eq('pebblebed.river.whatever')
  end

  context "in production" do
    subject { Pebbles::River::River.new(environment: 'production') }

    it "doesn't append the thing" do
      subject.connect
      subject.exchange.name.should eq('pebblebed.river')
    end
  end

  it "connects" do
    subject.connect
    subject.should be_connected
  end

  it "disconnects" do
    subject.connect
    subject.should be_connected
    subject.disconnect
    subject.should_not be_connected
  end

  it "connects if you try to publish something" do
    subject.should_not be_connected
    subject.publish(:event => :test, :uid => 'klass:path$123', :attributes => {:a => 'b'})
    subject.should be_connected
  end

  describe "publishing" do

    it "gets selected messages" do
      queue = subject.queue(:name => 'thingivore', :path => 'rspec', :klass => 'thing')

      queue.message_count.should eq(0)
      subject.publish(:event => 'smile', :uid => 'thing:rspec$1', :attributes => {:a => 'b'})
      subject.publish(:event => 'frown', :uid => 'thing:rspec$2', :attributes => {:a => 'b'})
      subject.publish(:event => 'laugh', :uid => 'thing:testunit$3', :attributes => {:a => 'b'})
      sleep(0.1)
      queue.message_count.should eq(2)
    end

    it "gets everything if it connects without a key" do
      queue = subject.queue(:name => 'omnivore')

      queue.message_count.should eq(0)
      subject.publish(:event => 'smile', :uid => 'thing:rspec$1', :attributes => {:a => 'b'})
      subject.publish(:event => 'frown', :uid => 'thing:rspec$2', :attributes => {:a => 'b'})
      subject.publish(:event => 'laugh', :uid => 'testunit:rspec$3', :attributes => {:a => 'b'})
      sleep(0.1)
      queue.message_count.should eq(3)
    end

    it "sends messages as json" do
      queue = subject.queue(:name => 'eatseverything')
      subject.publish(:event => 'smile', :source => 'rspec', :uid => 'klass:path$1', :attributes => {:a => 'b'})
      sleep(0.1)
      _, _, payload = queue.pop
      JSON.parse(payload)['uid'].should eq('klass:path$1')
    end

    CONNECTION_EXCEPTIONS.each do |exception_class|
      context "on temporary failure with #{exception_class}" do
        it "reconnects and retries sending until success" do
          exchange = double('exchange')

          count = 0
          exchange.stub(:publish) do
            count += 1
            if count < 3
              raise create_exception(exception_class)
            end
          end

          subject.stub(:connect) { }
          subject.stub(:exchange) { exchange }
          subject.stub(:sleep) { }
          Timeout.stub(:timeout) { |&block|
            block.call
          }
          expect(Timeout).to receive(:timeout).at_least(1).times

          expect(subject).to receive(:sleep).at_least(2).times
          expect(subject).to receive(:connect).exactly(3).times
          expect(subject).to receive(:disconnect).exactly(3).times

          expect(exchange).to receive(:publish).at_least(2).times

          subject.publish(event: 'explode', uid: 'thing:rspec$1')
        end
      end
    end

    CONNECTION_EXCEPTIONS.each do |exception_class|
      context "on permanent failure with #{exception_class}" do
        it "retries with exponential backoff until timeout and gives up with SendFailure" do
          exchange = double('exchange')
          exchange.stub(:publish) do
            raise create_exception(exception_class)
          end
          subject.stub(:exchange) { exchange }

          count, sleeps = 0, []
          subject.stub(:sleep) { |t|
            count += 1
            if count >= 10
              raise Timeout::Error
            end
            sleeps.push(t)
          }

          Timeout.stub(:timeout) { |&block|
            block.call
          }
          expect(Timeout).to receive(:timeout).at_least(1).times

          expect(subject).to receive(:disconnect).at_least(11).times

          expect(-> { subject.publish({event: 'explode', uid: 'thing:rspec$1'})}).to raise_error do |e|
            expect(e).to be_instance_of Pebbles::River::SendFailure
            expect(e.connection_exception.class).to eq exception_class
          end

          expect(sleeps[0, 9]).to eq [1, 2, 4, 8, 10, 10, 10, 10, 10]
        end
      end
    end

    context 'on connection timeout' do
      it "gives up with SendFailure" do
        exchange = double('exchange')
        exchange.stub(:publish) { }

        subject.stub(:exchange) { exchange }

        Timeout.stub(:timeout) { |&block|
          raise Timeout::Error, "execution expired"
        }
        expect(Timeout).to receive(:timeout).exactly(1).times

        expect(-> {
          subject.publish({event: 'explode', uid: 'thing:rspec$1'})
        }).to raise_error do |e|
          expect(e).to be_instance_of Pebbles::River::SendFailure
          expect(e.message).to eq 'Timeout'
          expect(e.connection_exception.class).to eq Timeout::Error
        end
      end
    end

  end

  it "subscribes" do
    queue = subject.queue(:name => 'alltestivore', :path => 'area51.rspec|area51.testunit|area52.*|area53.**', :klass => 'thing', :event => 'smile')

    queue.message_count.should eq(0)
    subject.publish(:event => 'smile', :uid => 'thing:area51.rspec$1', :attributes => {:a => 'b'})
    subject.publish(:event => 'smile', :uid => 'thing:area51.testunit$2', :attributes => {:a => 'b'})
    subject.publish(:event => 'smile', :uid => 'thing:area51.whatever$3', :attributes => {:a => 'b'}) # doesn't match path
    subject.publish(:event => 'frown', :uid => 'thing:area51.rspec$4', :attributes => {:a => 'b'}) # doesn't match event
    subject.publish(:event => 'smile', :uid => 'thing:area52.one.two.three$5', :attributes => {:a => 'b'}) # doesn't match wildcard path
    subject.publish(:event => 'smile', :uid => 'thing:area52.one$6', :attributes => {:a => 'b'}) # matches wildcard path
    subject.publish(:event => 'smile', :uid => 'thing:area53.one.two.three$7', :attributes => {:a => 'b'}) # matches wildcard path

    sleep(0.1)
    queue.message_count.should eq(4)
  end

  it "is a durable queue" do
    queue = subject.queue(:name => 'adurablequeue', :path => 'katrina')
    subject.publish(:event => 'test', :uid => 'person:katrina$1', :attributes => {:a => rand(1000)}, :persistent => false)
  end

end
