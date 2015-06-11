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

  let(:workers) { { 'test_worker' => 3 } }

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
      adapter.send(:set_max_worker_counts,workers)
    end

    it "sets global worker counts" do
      expect(client).to receive(:hsetnx).with("GRU:global:max_workers",'test_worker',3)
      adapter.send(:set_max_global_worker_counts,workers)
    end

  end

  context "Determining Available Workers" do

    it "gets all workers from redis" do
      expect(client).to receive(:hgetall).with("GRU:#{hostname}:max_workers").and_return({
        'test_worker' => 3
      })
      adapter.send(:max_host_workers)
    end

    context "Provisioning workers with same local and global max" do
      before(:each) do
        expect(client).to receive(:hgetall).with("GRU:#{hostname}:max_workers").and_return(workers)
        expect(client).to receive(:hget).with("GRU:#{hostname}:max_workers",'test_worker').exactly(1).times
        expect(client).to receive(:hget).with("GRU:global:max_workers",'test_worker').exactly(1).times
        expect(client).to receive(:hget).with("GRU:#{hostname}:workers_running",'test_worker').exactly(1).times
        expect(client).to receive(:hget).with("GRU:global:workers_running",'test_worker').exactly(1).times
      end

      it "returns workers with 0 existing workers" do
        expect(client).to receive(:multi).exactly(3).times.and_yield(client).and_return([0,0,3,3])
        expect(client).to receive(:setnx).exactly(3).times.and_return(true)
        expect(client).to receive(:del).with("GRU:test_worker").exactly(3).times
        expect(client).to receive(:hincrby).with("GRU:global:workers_running",'test_worker',1).exactly(3).times
        expect(client).to receive(:hincrby).with("GRU:#{hostname}:workers_running",'test_worker',1).exactly(3).times
        expect(client).to receive(:hget).with("GRU:#{hostname}:max_workers",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:global:max_workers",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:#{hostname}:workers_running",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:global:workers_running",'test_worker').exactly(2).times
        available_workers = adapter.provision_workers
        expect(available_workers).to eq({'test_worker' => 3})
      end

      it "returns workers when max local and global counts have not been reached" do
        expect(client).to receive(:multi).exactly(3).times.and_yield(client).and_return([1,1,3,3])
        expect(client).to receive(:setnx).exactly(3).times.and_return(true)
        expect(client).to receive(:del).with("GRU:test_worker").exactly(3).times
        expect(client).to receive(:hincrby).with("GRU:global:workers_running",'test_worker',1).exactly(3).times
        expect(client).to receive(:hincrby).with("GRU:#{hostname}:workers_running",'test_worker',1).exactly(3).times
        expect(client).to receive(:hget).with("GRU:#{hostname}:max_workers",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:global:max_workers",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:#{hostname}:workers_running",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:global:workers_running",'test_worker').exactly(2).times
        available_workers = adapter.provision_workers
        expect(available_workers).to eq({'test_worker' => 3})
      end

      it "does not return workers if max global count has been reached" do
        expect(client).to receive(:multi).exactly(3).times.and_yield(client).and_return([0,3,3,3])
        expect(client).to receive(:hget).with("GRU:#{hostname}:max_workers",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:global:max_workers",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:#{hostname}:workers_running",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:global:workers_running",'test_worker').exactly(2).times
        available_workers = adapter.provision_workers
        expect(available_workers).to eq({'test_worker' => 0})
      end

      it "doesn't return workers if max local count has been reached" do
        expect(client).to receive(:multi).exactly(3).times.and_yield(client).and_return([3,4,3,6])
        expect(client).to receive(:hget).with("GRU:#{hostname}:max_workers",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:global:max_workers",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:#{hostname}:workers_running",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:global:workers_running",'test_worker').exactly(2).times
        reserved_workers = adapter.provision_workers
        expect(reserved_workers).to eq({'test_worker' => 0})
      end

      it "doesn't return workers if global max is 0" do
        expect(client).to receive(:multi).exactly(3).times.and_yield(client).and_return([0,0,3,0])
        expect(client).to receive(:hget).with("GRU:#{hostname}:max_workers",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:global:max_workers",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:#{hostname}:workers_running",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:global:workers_running",'test_worker').exactly(2).times
        available_workers = adapter.provision_workers
        expect(available_workers).to eq({'test_worker' => 0})
      end

      it "doesn't provision workers if local max is 0" do
        expect(client).to receive(:multi).exactly(3).times.and_yield(client).and_return([0,1,0,3])
        expect(client).to receive(:hget).with("GRU:#{hostname}:max_workers",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:global:max_workers",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:#{hostname}:workers_running",'test_worker').exactly(2).times
        expect(client).to receive(:hget).with("GRU:global:workers_running",'test_worker').exactly(2).times
        available_workers = adapter.provision_workers
        expect(available_workers).to eq({'test_worker' => 0})
      end
    end
  end

  context "Determining Removeable Workers" do
    let(:workers) {
      { 'test_worker' => 1 }
    }

    before(:each) do
      expect(client).to receive(:hgetall).with("GRU:#{hostname}:max_workers").and_return(workers)
      expect(client).to receive(:hget).with("GRU:#{hostname}:max_workers",'test_worker').exactly(1).times
      expect(client).to receive(:hget).with("GRU:global:max_workers",'test_worker').exactly(1).times
      expect(client).to receive(:hget).with("GRU:global:workers_running",'test_worker').exactly(1).times
      expect(client).to receive(:hget).with("GRU:#{hostname}:workers_running",'test_worker').exactly(1).times
    end

    it "removes workers when local maximum has been exceeded" do
      expect(client).to receive(:multi).exactly(1).times.and_yield(client).and_return([3,3,1,3])
      expect(client).to receive(:setnx).exactly(1).times.and_return(true)
      expect(client).to receive(:del).with("GRU:test_worker").exactly(1).times
      expect(client).to receive(:hincrby).with("GRU:global:workers_running",'test_worker',-1).exactly(1).times
      expect(client).to receive(:hincrby).with("GRU:#{hostname}:workers_running",'test_worker',-1).exactly(1).times
      expect(adapter.expire_workers).to eq({'test_worker' => -1})
    end

    it "removes workers when global maximum has been exceeded" do
      expect(client).to receive(:multi).exactly(1).times.and_yield(client).and_return([3,3,3,1])
      expect(client).to receive(:setnx).exactly(1).times.and_return(true)
      expect(client).to receive(:del).with("GRU:test_worker").exactly(1).times
      expect(client).to receive(:hincrby).with("GRU:global:workers_running",'test_worker',-1).exactly(1).times
      expect(client).to receive(:hincrby).with("GRU:#{hostname}:workers_running",'test_worker',-1).exactly(1).times
      expect(adapter.expire_workers).to eq({'test_worker' => -1})
    end

    it "doesn't remove workers when local maximum has not been exceeded" do
      expect(client).to receive(:multi).exactly(1).times.and_yield(client).and_return([3,3,4,3])
      expect(adapter.expire_workers).to eq({'test_worker' => 0})
    end

    it "doesn't remove workers when global maximum has not been exceeded" do
      expect(client).to receive(:multi).exactly(1).times.and_yield(client).and_return([3,4,3,5])
      expect(adapter.expire_workers).to eq({'test_worker' => 0})
    end
  end
end
