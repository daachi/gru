require 'socket'

module Gru
  module Adapters
    class RedisAdapter
      attr_reader :client

      def initialize(client,global_config=nil)
        @client = client
        @global_config = Array(global_config)
      end

      def process_workers(workers)
        register_workers(workers)
        set_worker_counts(workers)
        set_global_worker_counts(workers)
      end

      def provision_workers
        workers = get_host_workers
        reserve_workers(workers)
      end

      def expired_workers; end

      private

      def register_workers(workers)
        workers.each {|worker| register_worker(worker) }
      end

      def set_worker_counts(workers)
        workers.each {|worker| set_worker_count(worker) }
      end

      def set_global_worker_counts(workers)
        workers.each {|worker| set_global_worker_count(worker) }
      end

      def register_worker(worker)
        send_message(:hsetnx,"#{host_key}:workers_running",worker['name'],0)
      end

      def set_worker_count(worker)
        send_message(:hsetnx,"#{host_key}:max_workers",worker['name'],worker['max_workers'])
      end

      def set_global_worker_count(worker)
        send_message(:hsetnx,"#{global_key}:max_workers",worker['name'],worker['max_workers'])
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

      def reserve_workers(workers)
        # Need a client.multi statement here to avoid race conditions?
        reserved_workers = []
        workers.each do |worker|
          worker['max_workers'].times do |index|
            if current_worker_count(worker) < max_worker_count(worker)
              send_message(:hincr,"#{host_key}:workers_running", worker['name'])
              reserved_workers << worker['name']
            end
          end
        end
        reserved_workers
      end

      def get_host_workers
        send_message(:hgetall,"#{host_key}:workers_running")
      end

      def get_host_worker_counts
        send_message(:hgetall,"#{host_key}:max_workers")
      end

      def current_worker_count(worker)
        send_message(:hget,"#{host_key}:workers_running",worker['name']).to_i
      end

      def max_worker_count(worker)
        send_message(:hget,"#{host_key}:max_workers",worker['name']).to_i
      end

      def get_global_worker_count(worker)
        send_message(:hget,"#{global_key}:workers_running",worker['name'])
      end

      def hostname
        @hostname ||= Socket.gethostname
      end

      def send_message(action,*args)
        @client.send(action,*args)
      end
    end
  end
end
