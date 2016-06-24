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

  context "max workers per host with high maximum worker count" do
    let(:settings) {
      {
        rebalance_flag: true,
        manage_worker_heartbeats: true,
        max_workers_per_host: nil,
        cluster_maximums: {
        'test_worker' => '100'
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

    let(:adapter4) {
      adapter4 = Gru::Adapters::RedisAdapter.new(Gru::Configuration.new(settings.clone))
      allow(adapter4).to receive(:hostname).and_return('test4')
      adapter4
    }

    it "doesn't exceed max processes per host setting" do
      test1 = Gru::WorkerManager.new(adapter1)
      test2 = Gru::WorkerManager.new(adapter2)
      test3 = Gru::WorkerManager.new(adapter3)
      test1.register_workers
      test2.register_workers
      test3.register_workers
      expect(test1.adjust_workers).to eq( { 'test_worker' => 30 })
      expect(test2.adjust_workers).to eq( { 'test_worker' => 30 })
      expect(test3.adjust_workers).to eq( { 'test_worker' => 30 })
    end

    it "honors the max processes per host setting and correctly rebalances worker counts" do
      test1 = Gru::WorkerManager.new(adapter1)
      test2 = Gru::WorkerManager.new(adapter2)
      test3 = Gru::WorkerManager.new(adapter3)
      test4 = Gru::WorkerManager.new(adapter4)
      test1.register_workers
      test2.register_workers
      test3.register_workers
      # Honors max processes per host setting
      expect(test1.adjust_workers).to eq({ 'test_worker' => 30 })
      expect(test2.adjust_workers).to eq({ 'test_worker' => 30 })
      expect(test3.adjust_workers).to eq({ 'test_worker' => 30 })
      test4.register_workers
      # Pick up remaining workers
      expect(test4.adjust_workers).to eq({ 'test_worker' => 10 })
      # Rebalance workers across hosts
      expect(test3.adjust_workers).to eq({ 'test_worker' => -5 })
      expect(test2.adjust_workers).to eq({ 'test_worker' => -5 })
      expect(test1.adjust_workers).to eq({ 'test_worker' => -5 })
      expect(test4.adjust_workers).to eq({ 'test_worker' => 15 })
      # No more workers to pick up on the hosts
      expect(test4.adjust_workers).to eq({ 'test_worker' => 0 })
      expect(test3.adjust_workers).to eq({ 'test_worker' => 0 })
      expect(test2.adjust_workers).to eq({ 'test_worker' => 0 })
      expect(test1.adjust_workers).to eq({ 'test_worker' => 0 })
      expect(test4.adjust_workers).to eq({ 'test_worker' => 0 })
      # Release workers
      test4.release_workers
      # Rebalance for lost host
      expect(test1.adjust_workers).to eq({ 'test_worker' => 5 })
      expect(test2.adjust_workers).to eq({ 'test_worker' => 5 })
      expect(test3.adjust_workers).to eq({ 'test_worker' => 5 })
      # No more workers due to max per host limit
      expect(test1.adjust_workers).to eq({ 'test_worker' => 0 })
      expect(test2.adjust_workers).to eq({ 'test_worker' => 0 })
      expect(test3.adjust_workers).to eq({ 'test_worker' => 0 })
    end

  end

  context "max workers per host with high maximum worker count" do
    let(:settings) {
      {
        rebalance_flag: true,
        manage_worker_heartbeats: true,
        max_workers_per_host: 5,
        cluster_maximums: {
        'test_worker1' => '3',
        'test_worker2' => '3',
        'test_worker3' => '3',
        'test_worker4' => '3',
        'test_worker5' => '3',
        'test_worker6' => '3'
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

    let(:adapter4) {
      adapter4 = Gru::Adapters::RedisAdapter.new(Gru::Configuration.new(settings.clone))
      allow(adapter4).to receive(:hostname).and_return('test4')
      adapter4
    }

    it "handles provisioning with many workers and few worker instances" do
      test1 = Gru::WorkerManager.new(adapter1)
      test2 = Gru::WorkerManager.new(adapter2)
      test3 = Gru::WorkerManager.new(adapter3)
      test4 = Gru::WorkerManager.new(adapter4)
      test1.register_workers
      test2.register_workers
      test3.register_workers
      # Honors max processes per host setting
      expect(test1.adjust_workers).to eq({
        'test_worker1' => 1,
        'test_worker2' => 1,
        'test_worker3' => 1,
        'test_worker4' => 1,
        'test_worker5' => 1,
        'test_worker6' => 0
      })

      expect(test2.adjust_workers).to eq({
        'test_worker1' => 1,
        'test_worker2' => 1,
        'test_worker3' => 1,
        'test_worker4' => 1,
        'test_worker5' => 1,
        'test_worker6' => 0
      })

      expect(test3.adjust_workers).to eq({
        'test_worker1' => 1,
        'test_worker2' => 1,
        'test_worker3' => 1,
        'test_worker4' => 1,
        'test_worker5' => 1,
        'test_worker6' => 0
      })
      test4.register_workers
      # Adds new worker based on new instance
      expect(test4.adjust_workers).to eq({
        'test_worker1' => 0,
        'test_worker2' => 0,
        'test_worker3' => 0,
        'test_worker4' => 0,
        'test_worker5' => 0,
        'test_worker6' => 1
      })

      # Doesn't alter existing worker instances
      # due to max_workers_per_host balance
      expect(test1.adjust_workers).to eq({
        'test_worker1' => 0,
        'test_worker2' => 0,
        'test_worker3' => 0,
        'test_worker4' => 0,
        'test_worker5' => 0,
        'test_worker6' => 0
      })
    end

    it "handles expiring with many workers and few worker instances" do
      test1 = Gru::WorkerManager.new(adapter1)
      test1.register_workers
      args = ["GRU:default:default:test1:workers_running",
              'test_worker1',1,'test_worker2',1,'test_worker3',1,
              'test_worker4',1,'test_worker5',1,'test_worker6',1
             ]
      client.hmset(*args)

      # Honors max processes per host setting
      expect(test1.adjust_workers).to eq({
        'test_worker1' => -1,
        'test_worker2' => 0,
        'test_worker3' => 0,
        'test_worker4' => 0,
        'test_worker5' => 0,
        'test_worker6' => 0
      })

      expect(test1.adjust_workers).to eq({
        'test_worker1' => 0,
        'test_worker2' => 0,
        'test_worker3' => 0,
        'test_worker4' => 0,
        'test_worker5' => 0,
        'test_worker6' => 0
      })
    end
  end
  context "max workers per host without rebalance flag" do
    let(:settings) {
      {
        rebalance_flag: false,
        manage_worker_heartbeats: true,
        max_workers_per_host: 5,
        cluster_maximums: {
        'test_worker1' => '3',
        'test_worker2' => '3',
        'test_worker3' => '3',
        'test_worker4' => '3',
        'test_worker5' => '3',
        'test_worker6' => '3'
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

    let(:adapter4) {
      adapter4 = Gru::Adapters::RedisAdapter.new(Gru::Configuration.new(settings.clone))
      allow(adapter4).to receive(:hostname).and_return('test4')
      adapter4
    }

    it "honors the max workers per host setting" do
      test1 = Gru::WorkerManager.new(adapter1)
      test2 = Gru::WorkerManager.new(adapter2)
      test3 = Gru::WorkerManager.new(adapter3)
      test4 = Gru::WorkerManager.new(adapter4)
      test1.register_workers
      test2.register_workers
      test3.register_workers
      # Honors max processes per host setting
      expect(test1.adjust_workers).to eq({
        'test_worker1' => 3,
        'test_worker2' => 2,
        'test_worker3' => 0,
        'test_worker4' => 0,
        'test_worker5' => 0,
        'test_worker6' => 0
      })

      expect(test2.adjust_workers).to eq({
        'test_worker1' => 0,
        'test_worker2' => 1,
        'test_worker3' => 3,
        'test_worker4' => 1,
        'test_worker5' => 0,
        'test_worker6' => 0
      })

      expect(test3.adjust_workers).to eq({
        'test_worker1' => 0,
        'test_worker2' => 0,
        'test_worker3' => 0,
        'test_worker4' => 2,
        'test_worker5' => 3,
        'test_worker6' => 0
      })

      test4.register_workers
      expect(test4.adjust_workers).to eq({
        'test_worker1' => 0,
        'test_worker2' => 0,
        'test_worker3' => 0,
        'test_worker4' => 0,
        'test_worker5' => 0,
        'test_worker6' => 3
      })
    end
  end

  context "Remove Stale Workers" do
    let(:new_settings) {
      {
        cluster_maximums: {
        'foo_worker' => '3'
        }
      }
    }

    let(:adapter1) {
      adapter1 = Gru::Adapters::RedisAdapter.new(Gru::Configuration.new(settings.clone))
      allow(adapter1).to receive(:hostname).and_return('test1')
      adapter1
    }

    let(:adapter2) {
      adapter2 = Gru::Adapters::RedisAdapter.new(Gru::Configuration.new(new_settings.clone))
      allow(adapter2).to receive(:hostname).and_return('test2')
      adapter2
    }

    it "removes stale worker keys" do
      test1 = Gru::WorkerManager.new(adapter1)
      test2 = Gru::WorkerManager.new(adapter2)
      test1.register_workers
      test2.register_workers
      expect(client.hgetall("GRU:default:default:global:max_workers").keys).to eq(['foo_worker'])
    end
  end
end
