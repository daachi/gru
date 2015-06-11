require 'gru/version'
require 'gru/worker_manager'
require 'gru/adapters'
require 'gru/adapters/redis_adapter'

module Gru
  def self.with_redis_connection(client, worker_config, global_config)
    manager = WorkerManager.with_redis_connection(client,worker_config,global_config)
    manager.register_worker_queues
    manager
  end
end
