require 'rspec'
require_relative '../../../lib/gru'

describe Gru::Adapters::RedisAdapter do
  before(:each) do
    allow(Socket).to receive(:gethostname).and_return(hostname)
  end

  let(:hostname) { 'foo' }
  let(:client) { double('client') }

  let(:adapter) {
    Gru::Adapters::RedisAdapter.new(client)
  }

  let(:workers) {
    [ { 'name' => 'test_worker', 'max_workers' => 3, 'min_workers' => 1 } ]
  }

  context "initialization" do
    it "has a client" do
      expect(adapter.client).to eq(client)
    end
  end

  context "processing workers" do

    it "determines the host key" do
      expect(adapter.send(:host_key)).to eq("GRU:#{hostname}")
    end

    it "registers workers" do
      expect(client).to receive(:hsetnx).with("GRU:#{hostname}:workers_running",'test_worker',0)
      adapter.send(:register_workers,workers)
    end

    it "sets worker counts" do
      expect(client).to receive(:hsetnx).with("GRU:#{hostname}:max_workers",'test_worker',3)
      adapter.send(:set_worker_counts,workers)
    end

    it "sets global worker counts" do
      expect(client).to receive(:hsetnx).with("GRU:global:max_workers",'test_worker',3)
      adapter.send(:set_global_worker_counts,workers)
    end

  end

  context "provisioning workers" do

    it "gets all workers from redis" do
      expect(client).to receive(:hgetall).with("GRU:#{hostname}:workers_running").and_return({
        'test_worker' => 0
      })
      adapter.send(:get_host_workers)
    end

    it "reserves running workers in redis" do
      expect(client).to receive(:hget).with("GRU:#{hostname}:workers_running",'test_worker').exactly(3).times.and_return(0,1,2)
      expect(client).to receive(:hget).with("GRU:#{hostname}:max_workers",'test_worker').exactly(3).times.and_return(3)
      expect(client).to receive(:hincr).with("GRU:#{hostname}:workers_running",'test_worker').exactly(3).times.and_return(true)
      reserved_workers = adapter.send(:reserve_workers,workers)
      expect(reserved_workers).to eq(['test_worker','test_worker','test_worker'])
    end

  end
end
