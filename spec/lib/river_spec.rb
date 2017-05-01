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
