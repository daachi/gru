module Gru
  class WorkerManager
    attr_reader :adapter

    def initialize(adapter, hooks)
      @adapter = adapter
      @hooks = hooks
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
      results = {}
      @hooks.each do |hook|
        results = hook.new(adapter,results).call
      end
      results
    end

    def release_workers
      @adapter.release_workers
    end

  end
end
