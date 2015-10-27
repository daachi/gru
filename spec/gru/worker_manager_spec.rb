require 'rspec'
require './lib/gru'

describe Gru::WorkerManager do
  let (:adapter) {
    double("adapter")
  }

  let(:manager) {
    Gru::WorkerManager.new(adapter)
  }

  let(:workers) {
    {}
  }

  context "When initialized" do
    it "has an adapter instance" do
      expect(manager.adapter).not_to be_nil
    end
  end

  context "Creating Worker Queues" do
    it "Creates workers" do
      expect(adapter).to receive(:set_worker_counts).and_return(true)
      manager.register_workers
    end
  end

  context "Finding available or removeable workers" do
    it "determines new workers to create" do
      expect(adapter).to receive(:provision_workers).and_return({})
      expect(adapter).to receive(:expire_workers).and_return({})
      expect(adapter).to receive(:release_presumed_dead_workers)
      manager.adjust_workers
    end

    it "determines workers to expire" do
      expect(adapter).to receive(:expire_workers).and_return(instance_of(Array))
      manager.expire_workers
    end
  end
end
