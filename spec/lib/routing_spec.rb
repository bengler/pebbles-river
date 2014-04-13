require 'spec_helper'

describe Pebbles::River::Routing do

  subject do
    Pebbles::River::Routing
  end

  describe "routing keys" do

    specify do
      options = {:event => 'created', :uid => 'post.awesome.event:feeds.bagera.whatevs$123'}
      subject.routing_key_for(options).should eq('created._.post.awesome.event._.feeds.bagera.whatevs')
    end

    specify "event is required" do
      ->{ subject.routing_key_for(:uid => 'whatevs') }.should raise_error ArgumentError
    end

    specify "uid is required" do
      ->{ subject.routing_key_for(:event => 'whatevs') }.should raise_error ArgumentError
    end

  end
end
