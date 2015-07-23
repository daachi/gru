require 'gru/adapters'
require 'gru/adapters/redis_adapter'

module Gru
  class Configuration
    attr_reader :cluster_maximums, :host_maximums, :rebalance_flag, :adapter, :cluster_name, :environment_name
    def initialize(settings)
      @host_maximums = settings.delete(:host_maximums) || settings.delete(:cluster_maximums)
      @cluster_maximums = settings.delete(:cluster_maximums) || @host_maximums
      @rebalance_flag = settings.delete(:rebalance_flag) || false
      @cluster_name = settings.delete(:cluster_name) || 'default'
      @environment_name = settings.delete(:environment_name) || 'default'
      client = initialize_client(settings.delete(:redis_config))
      @adapter = Gru::Adapters::RedisAdapter.new(client,self)
      if @cluster_maximums.nil?
        raise ArgumentError "Need at least a cluster configuration"
      end
    end

    private

    def initialize_client(config=nil)
      Redis.new(config || {})
    end
  end
end
