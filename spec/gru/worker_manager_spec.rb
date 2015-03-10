require 'rspec'
require './lib/gru'

describe Gru::WorkerManager do
  let (:client) {
    double("client")
  }

  let(:manager) {
    Gru::WorkerManager.new(client, workers)
  }

  let(:workers) {
    []
  }

  context "When initialized" do
    it "has workers" do
      expect(manager.workers).not_to be_nil
    end

    it "has a client instance" do
      expect(manager.adapter).not_to be_nil
    end
  end

  context "Creating workers" do
    it "Creates workers" do
      expect(client).to receive(:provision_workers).with(workers).and_return(true)
      manager.create_worker_queues
    end
  end

  context "Adjusting worker counts" do
    it "determines new workers to create" do
      expect(client).to receive(:new_workers).and_return(instance_of(Array))
      manager.workers_to_create
    end

    it "determines workers to expire" do
      expect(client).to receive(:expired_workers).and_return(instance_of(Array))
      manager.workers_to_delete
    end
  end
end
