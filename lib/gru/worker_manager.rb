module Gru
  class WorkerManager
    attr_reader :workers, :adapter

    def initialize(adapter,workers)
      @adapter = adapter
      @workers = workers
    end

    def create_worker_queues
      @adapter.provision_workers(@workers)
    end

    def workers_to_create
      @adapter.new_workers
    end

    def workers_to_delete
      @adapter.expired_workers
    end
  end
end
