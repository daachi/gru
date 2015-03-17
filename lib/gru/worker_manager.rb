module Gru
  class WorkerManager
    attr_reader :workers, :adapter

    def self.create(settings,workers)
      redis = Redis.new(settings)
      adapter = Gru::Adapters::RedisAdapter.new(redis)
      new(adapter,workers)
    end

    def self.with_redis_connection(client,workers,global_config=nil)
      adapter = Gru::Adapters::RedisAdapter.new(client,global_config)
      new(adapter,workers)
    end

    def initialize(adapter,workers)
      @adapter = adapter
      @workers = workers
    end

    def register_worker_queues
      @adapter.process_workers(@workers)
    end

    def provision_workers
      @adapter.provision_workers
    end

    def expire_workers
      @adapter.expire_workers
    end

    def adjust_workers
      result = {}
      add = provision_workers
      remove = expire_workers
      keys = add.keys + remove.keys
      keys.uniq.each do |key|
        result[key] = add.fetch(key) {0} + remove.fetch(key) {0}
      end
      result
    end

    def release_workers
      @adapter.release_workers
    end

    def cleanup
      @adapter.cleanup
    end

  end
end
