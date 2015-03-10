require 'socket'

module Gru
  module Adapters
    class RedisAdapter
      attr_reader :client

      def initialize(client)
        @client = client
      end

      def provision_workers(workers)
        workers.each do |worker|
          provision_worker(worker)
          set_minimum_workers_count(worker)
          set_maximum_workers_count(worker)
        end
      end

      def start_workers; end
      def expired_workers; end

      private

      def provision_worker(worker)
        @client.hsetnx(host_key,worker['name'],0)
      end

      def set_minimum_workers_count(worker)
        @client.hsetnx(host_key,"#{worker['name']}:minimum",worker['min_workers'])
      end

      def set_maximum_workers_count(worker)
        @client.hsetnx(host_key,"#{worker['name']}:maximum",worker['max_workers'])
      end

      def set_global_minimum_worker_count(worker,minimum)
        @client.hsetnx(global_key,"#{worker['name']}:minimum",minimum)
      end

      def set_global_maximum_worker_count(worker,maximum)
        @client.hsetnx(global_key,"#{worker['name']}:maximum",maximum)
      end

      def workers
        # SADD workers to set
        # GET SET for all workers
        # LOOP through workers
        # for each worker get min and max count
        # for each worker get global min and max count
        # only use max count
        # if max count = -1 use global max
        # if max count = 0 then 0 workers
        # else incr worker count until <= local max and <= global max
        # next worker
      end

      def global_key
        "GRU:global"
      end

      def host_key
        "GRU:#{hostname}"
      end

      def hostname
        Socket.gethostname
      end
    end
  end
end
