module Gru
  class WorkerManager
    attr_reader :adapter

    def initialize(adapter)
      @adapter = adapter
    end

    def register_workers
      @adapter.set_worker_counts
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

  end
end
