require 'gru/adapters'
require 'gru/adapters/redis_adapter'

module Gru
  class Configuration
    attr_reader :cluster_maximums, :host_maximums, :rebalance_flag, :adapter, :cluster_name, :environment_name, :presume_host_dead_after, :client_settings, :manage_worker_heartbeats
    def initialize(settings)
      @host_maximums = settings.delete(:host_maximums) || settings.delete(:cluster_maximums)
      @cluster_maximums = settings.delete(:cluster_maximums) || @host_maximums
      @rebalance_flag = settings.delete(:rebalance_flag) || false
      @cluster_name = settings.delete(:cluster_name) || 'default'
      @environment_name = settings.delete(:environment_name) || 'default'
      @presume_host_dead_after = settings.delete(:presume_host_dead_after)
      @client_settings = settings.delete(:client_settings)
      @manage_worker_heartbeats = settings.delete(:manage_worker_heartbeats) || false
      @max_worker_processes_per_host = settings.delete(:max_workers_per_host) || 30
      @adapter = Gru::Adapters::RedisAdapter.new(self)
      if @cluster_maximums.nil?
        raise ArgumentError, "Need at least a cluster configuration"
      end
    end
  end
end
