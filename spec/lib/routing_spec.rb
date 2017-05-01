require 'spec_helper'

describe Pebbles::River::Routing do

  subject do
    Pebbles::River::Routing
  end

  describe '#routing_key_for' do
    specify do
      options = {event: 'created', :uid => 'post.awesome.event:feeds.bagera.whatevs$123'}
      subject.routing_key_for(options).should eq('created._.post.awesome.event._.feeds.bagera.whatevs')
    end

    specify 'event is required' do
      ->{ subject.routing_key_for(:uid => 'whatevs') }.should raise_error ArgumentError
    end

    specify 'uid is required' do
      ->{ subject.routing_key_for(event: 'whatevs') }.should raise_error ArgumentError
    end
  end

  describe '#binding_routing_keys_for' do
    specify 'simple, direct match' do
      expect(subject.binding_routing_keys_for(event: 'create', class: 'post.event', path: 'feed.bagera')).to eq [
        'create._.post.event._.feed.bagera'
      ]
    end

    specify 'simple wildcard match' do
      options = {event: '*.create', class: 'post.*', path: '*.bagera.*'}
      expect(subject.binding_routing_keys_for(options)).to eq(['*.create._.post.*._.*.bagera.*'])
    end

    describe "anything matchers" do
      specify 'match anything (duh)' do
        expect(subject.binding_routing_keys_for({event: '**', class: '**', path: '**'})).to eq(['#._.#._.#'])
      end

      specify 'match nothing if not specified' do
        expect(subject.binding_routing_keys_for({})).to eq ["#._.#._.#"]
      end
    end

    it 'handles "or" queries' do
      options = {event: 'create|delete', class: 'post', path: 'bagera|bandwagon'}
      expected = ['create._.post._.bagera', 'delete._.post._.bagera', 'create._.post._.bandwagon', 'delete._.post._.bandwagon'].sort
      expect(subject.binding_routing_keys_for(options).sort).to eq(expected)
    end

    # FIXME
    # describe "optional paths" do
    #   it { Subscription.new.pathify('a.b').should eq(['a.b']) }
    #   it { Subscription.new.pathify('a.^b.c').should eq(%w(a a.b a.b.c)) }
    # end

    it "handles optional queries" do
      options = {event: 'create', class: 'post', path: 'feeds.bagera.^fb.concerts'}
      expected = ['create._.post._.feeds.bagera', 'create._.post._.feeds.bagera.fb', 'create._.post._.feeds.bagera.fb.concerts'].sort
      expect(subject.binding_routing_keys_for(options).sort).to eq(expected)
    end

    it "combines all kinds of weird stuff" do
      options = {event: 'create', class: 'post', path: 'a.^b.c|x.^y.z'}
      expected = [
        'create._.post._.a',
        'create._.post._.a.b',
        'create._.post._.a.b.c',
        'create._.post._.x',
        'create._.post._.x.y',
        'create._.post._.x.y.z',
      ].sort
      expect(subject.binding_routing_keys_for(options).sort).to eq(expected)
    end
  end
end
