require 'rspec'
require './lib/gru'

describe Gru::WorkerManager do
  let (:adapter) {
    double("adapter")
  }

  let(:manager) {
    Gru::WorkerManager.new(adapter, workers)
  }

  let(:workers) {
    {}
  }

  context "When initialized" do
    it "has workers" do
      expect(manager.workers).not_to be_nil
    end

    it "has an adapter instance" do
      expect(manager.adapter).not_to be_nil
    end
  end

  context "Creating Worker Queues" do
    it "Creates workers" do
      expect(adapter).to receive(:process_workers).with(workers).and_return(true)
      manager.register_worker_queues
    end
  end

  context "Finding available or removeable workers" do
    it "determines new workers to create" do
      expect(adapter).to receive(:provision_workers).and_return({})
      expect(adapter).to receive(:expire_workers).and_return({})
      manager.adjust_workers
    end

    it "determines workers to expire" do
      expect(adapter).to receive(:expire_workers).and_return(instance_of(Array))
      manager.expire_workers
    end
  end
end
