require 'rspec'
require_relative '../../../lib/gru'

describe Gru::Adapters::RedisAdapter do
  let(:hostname) {
    'foo'
  }

  let(:client) {
    double("client")
  }

  let(:adapter) {
    Gru::Adapters::RedisAdapter.new(client)
  }

  context "initialization" do
    it "has a client" do
      expect(adapter.client).to eq(client)
    end
  end

  context "creating queues" do
    let(:workers) {
      [ { 'name' => 'test_worker', 'max_workers' => 3, 'min_workers' => 1 } ]
    }

    it "determines the host key" do
      allow(Socket).to receive(:gethostname).and_return(hostname)
      expect(adapter.send(:host_key)).to eq("GRU:#{hostname}")
    end

    it "creates worker queues" do
      allow(Socket).to receive(:gethostname).and_return(hostname)
      expect(client).to receive(:hsetnx).with("GRU:#{hostname}",'test_worker',0)
      expect(client).to receive(:hsetnx).with("GRU:#{hostname}",'test_worker:minimum',1)
      expect(client).to receive(:hsetnx).with("GRU:#{hostname}",'test_worker:maximum',3)
      adapter.provision_workers(workers)
    end
  end

  context "provisions workers" do


  end

end
