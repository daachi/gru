require './lib/gru.rb'
require 'socket'
require 'digest'
require 'redis'
require 'pry'


# This requires a redis server instance running on localhost
# un-pend to run with actual redis-server
xdescribe Gru do

  after {
    client.flushdb
  }

  let(:settings) {
    {
      cluster_maximums: {
      'test_worker' => '3'
      }
    }
  }

  let(:client) {
    Redis.new
  }

  let(:hostname) {
    Socket.gethostname
  }

  let(:host_key) {
    "GRU:default:default:#{hostname}"
  }

  let(:global_key) {
    "GRU:default:default:global"
  }

  context "configuration" do
    it "sets configuration in redis" do
      manager = Gru.create(settings.clone)
      expect(client.hgetall("#{host_key}:max_workers")).to eq(settings[:cluster_maximums])
      expect(manager.provision_workers).to eq({ 'test_worker' => 3 })
      expect(client.hgetall("#{host_key}:workers_running")).to eq(settings[:cluster_maximums])
    end
  end

  context "adjusting worker counts" do
    it "adjusts worker counts based on changing global counts" do
      manager = Gru.create(settings.clone)
      expect(manager.provision_workers).to eq({ 'test_worker' => 3 })
      client.hset("#{global_key}:max_workers", 'test_worker', 1)
      expect(manager.adjust_workers).to eq({ 'test_worker' => -2})
      expect(manager.adjust_workers).to eq({ 'test_worker' => 0})
      expect(client.hget("#{global_key}:workers_running", 'test_worker')).to eq("1")
      client.hset("#{global_key}:max_workers", 'test_worker', 3)
      expect(manager.adjust_workers).to eq({ 'test_worker' => 2})
      expect(client.hget("#{global_key}:workers_running", 'test_worker')).to eq("3")
    end

    it "adjusts worker counts based on changing local counts" do
      manager = Gru.create(settings.clone)
      expect(manager.provision_workers).to eq({ 'test_worker' => 3 })
      client.hset("#{host_key}:max_workers", 'test_worker', 1)
      expect(manager.adjust_workers).to eq({ 'test_worker' => -1})
      expect(manager.adjust_workers).to eq({ 'test_worker' => -1})
      expect(manager.adjust_workers).to eq({ 'test_worker' => 0})
      expect(client.hget("#{global_key}:workers_running", 'test_worker')).to eq("1")
      expect(client.hget("#{host_key}:workers_running", 'test_worker')).to eq("1")
    end

    it "does not exceed local maximum counts" do
      manager = Gru.create(settings.clone)
      expect(manager.provision_workers).to eq({ 'test_worker' => 3 })
      client.hset("#{host_key}:max_workers", 'test_worker', 2)
      expect(manager.adjust_workers).to eq({ 'test_worker' => -1})
      expect(manager.adjust_workers).to eq({ 'test_worker' => 0})
      client.hset("#{global_key}:max_workers", 'test_worker', 4)
      expect(manager.adjust_workers).to eq({ 'test_worker' => 0})
      expect(client.hget("#{global_key}:workers_running", 'test_worker')).to eq("2")
      expect(client.hget("#{host_key}:workers_running", 'test_worker')).to eq("2")
    end

    it "does not exceed global maximum counts" do
      manager = Gru.create(settings.clone)
      expect(manager.provision_workers).to eq({ 'test_worker' => 3 })
      client.hset("#{host_key}:max_workers", 'test_worker', 0)
      expect(manager.adjust_workers).to eq({ 'test_worker' => 0})
      client.hset("#{global_key}:max_workers", 'test_worker', 3)
      expect(manager.adjust_workers).to eq({ 'test_worker' => 0})
      client.hset("#{host_key}:max_workers", 'test_worker', 4)
      expect(manager.adjust_workers).to eq({ 'test_worker' => 0})
      expect(client.hget("#{global_key}:workers_running", 'test_worker')).to eq("3")
      expect(client.hget("#{host_key}:workers_running", 'test_worker')).to eq("3")
    end
  end

  context "multiple workers" do
    let(:settings) {
      {
        rebalance_flag: true,
        manage_worker_heartbeats: true,
        cluster_maximums: {
        'test_worker' => '6'
        }
      }
    }

    let(:test_client) {
      Redis.new
    }

    let(:adapter1) {
      adapter1 = Gru::Adapters::RedisAdapter.new(Gru::Configuration.new(settings.clone))
      allow(adapter1).to receive(:hostname).and_return('test1')
      adapter1
    }

    let(:adapter2) {
      adapter2 = Gru::Adapters::RedisAdapter.new(Gru::Configuration.new(settings.clone))
      allow(adapter2).to receive(:hostname).and_return('test2')
      adapter2
    }

    let(:adapter3) {
      adapter3 = Gru::Adapters::RedisAdapter.new(Gru::Configuration.new(settings.clone))
      allow(adapter3).to receive(:hostname).and_return('test3')
      adapter3
    }

    it "adjusts workers when new hosts are added" do
      test1 = Gru::WorkerManager.new(adapter1)
      test2 = Gru::WorkerManager.new(adapter2)
      test3 = Gru::WorkerManager.new(adapter3)
      test1.register_workers
      test2.register_workers
      expect(test_client.hget('GRU:default:default:test1:workers_running', 'test_worker')).to eq('0')
      expect(test1.adjust_workers).to eq( { 'test_worker' => 3 })
      expect(test2.adjust_workers).to eq( { 'test_worker' => 3 })
      test3.register_workers
      expect(test1.adjust_workers).to eq( { 'test_worker' => -1 })
      expect(test1.adjust_workers).to eq( { 'test_worker' => 0 })
      expect(test3.adjust_workers).to eq( { 'test_worker' => 1 })
      expect(test3.adjust_workers).to eq( { 'test_worker' => 0 })
      expect(test2.adjust_workers).to eq( { 'test_worker' => -1 })
      expect(test2.adjust_workers).to eq( { 'test_worker' => 0 })
      expect(test3.adjust_workers).to eq( { 'test_worker' => 1 })
      expect(test3.adjust_workers).to eq( { 'test_worker' => 0 })
      expect(test1.adjust_workers).to eq( { 'test_worker' => 0 })
      expect(test2.adjust_workers).to eq( { 'test_worker' => 0 })
    end

    it "recovers lost worker counts" do
      test1 = Gru::WorkerManager.new(adapter1)
      test2 = Gru::WorkerManager.new(adapter2)
      test3 = Gru::WorkerManager.new(adapter3)
      test1.register_workers
      test2.register_workers
      expect(test1.adjust_workers).to eq( { 'test_worker' => 3 })
      expect(test2.adjust_workers).to eq( { 'test_worker' => 3 })
      test_client.hset("GRU:default:default:heartbeats", 'test1', Time.now - 300)
      expect(test2.adjust_workers).to eq( { 'test_worker' => 3 })
      test3.register_workers
      expect(test3.adjust_workers).to eq( { 'test_worker' => 0 })
      expect(test2.adjust_workers).to eq( { 'test_worker' => -3 })
      test_client.hset("GRU:default:default:heartbeats", 'test3', Time.now - 300)
      expect(test2.adjust_workers).to eq( { 'test_worker' => 3 })
    end
  end
end
