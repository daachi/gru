require 'socket'

module Gru
  module Adapters
    class RedisAdapter
      attr_reader :client

      def initialize(client,global_config=nil)
        @client = client
        @global_config = global_config
      end

      def process_workers(workers)
        register_workers(workers)
        set_max_worker_counts(workers)
        register_global_workers(@global_config || workers)
        set_max_global_worker_counts(@global_config || workers)
      end

      def provision_workers(balanced=false)
        available = {}
        workers = max_host_workers
        workers.each do |worker, count|
          i = 0
          Integer(count).times do
            if reserve_worker?(worker,balanced)
              i += 1 if reserve_worker(worker)
            end
          end
          available[worker] = i
        end
        available
      end

      def expire_workers(balanced=false)
        removable = {}
        workers = max_host_workers
        workers.each do |worker, count|
          i = 0
          Integer(count).times do
            if expire_worker?(worker,balanced)
              i -= 1 if expire_worker(worker)
            end
          end
          removable[worker] = i
        end
        removable
      end

      def release_workers
        workers = max_host_workers
        workers.keys.each do |worker|
          host_running_count = local_running_count(worker)
          running_count = host_running_count
          global_running_count = host_running_count
          host_running_count.times do
            if global_running_count > 0
              global_running_count = send_message(:hincrby, global_workers_running_key,worker,-1)
            end
            if running_count > 0
              running_count = send_message(:hincrby, host_workers_running_key,worker,-1)
            end
          end
        end
        send_message(:del, host_workers_running_key)
        send_message(:del, host_max_worker_key)
      end

      private

      def register_workers(workers)
        workers.each {|worker, count| register_worker(worker,0) }
      end

      def register_global_workers(workers)
        workers.each {|worker, count| register_global_worker(worker,0) }
      end

      def set_max_worker_counts(workers)
        workers.each_pair {|worker,count| set_max_worker_count(worker,count) }
      end

      def set_max_global_worker_counts(workers)
        reset_removed_global_worker_counts(workers)
        workers.each_pair{|worker,count| set_max_global_worker_count(worker,count) }
      end

      def register_worker(worker,count)
        send_message(:hsetnx,"#{host_key}:workers_running",worker,count)
      end

      def register_global_worker(worker,count)
        send_message(:hsetnx,"#{global_key}:workers_running",worker,count)
      end

      def set_max_worker_count(worker,count)
        send_message(:hset,"#{host_key}:max_workers",worker,count)
      end

      def set_max_global_worker_count(worker,count)
        send_message(:hset,"#{global_key}:max_workers",worker,count)
      end

      def reset_removed_global_worker_counts(workers)
        global_max = send_message(:hgetall, global_max_worker_key)
        global_max.each_pair do |worker, count|
          send_message(:hset, global_max_worker_key, worker, 0) unless workers[worker]
        end
      end

      def reserve_worker(worker)
        adjust_workers(worker,1)
      end

      def expire_worker(worker)
        adjust_workers(worker,-1)
      end

      def adjust_workers(worker,amount)
        lock_key = "GRU:#{worker}"
        if send_message(:setnx,lock_key,Time.now.to_i)
          send_message(:hincrby,host_workers_running_key,worker,amount)
          send_message(:hincrby,global_workers_running_key,worker,amount)
          send_message(:del,lock_key)
          return true
        end
        false
      end

      def max_host_workers
        send_message(:hgetall,host_max_worker_key)
      end

      def reserve_worker?(worker,balanced)
        host_running,global_running,host_max,global_max = worker_counts(worker)
        result = false
        if balanced
          result = host_running.to_i < max_workers_per_host(global_max,host_max)
        else
          result = host_running.to_i < host_max.to_i
        end
          result && global_running.to_i < global_max.to_i
      end

      def expire_worker?(worker,balanced)
        host_running,global_running,host_max,global_max = worker_counts(worker)
        result = false
        if balanced
          result = host_running.to_i > max_workers_per_host(global_max,host_max)
        else
          result = host_running.to_i > host_max.to_i
        end
         (result || global_running.to_i > global_max.to_i) && host_running.to_i >= 0
      end

      def worker_counts(worker)
        @client.multi do |multi|
          multi.hget(host_workers_running_key,worker)
          multi.hget(global_workers_running_key,worker)
          multi.hget(host_max_worker_key,worker)
          multi.hget(global_max_worker_key,worker)
        end
      end

      def local_running_count(worker)
        send_message(:hget,host_workers_running_key,worker).to_i
      end

      def gru_host_count
        send_message(:keys,"GRU:*:workers_running").count - 1
      end

      def max_workers_per_host(global_worker_max_count,host_max)
        host_count = gru_host_count
        rebalance_count = host_count > 0 ? (global_worker_max_count.to_i/host_count.to_f).ceil : host_max.to_i
        rebalance_count <= host_max.to_i && host_count > 1 ? rebalance_count : host_max.to_i
      end

      def host_max_worker_key
        "#{host_key}:max_workers"
      end

      def host_workers_running_key
        "#{host_key}:workers_running"
      end

      def global_max_worker_key
        "#{global_key}:max_workers"
      end

      def global_workers_running_key
        "#{global_key}:workers_running"
      end

      def global_key
        "GRU:global"
      end

      def host_key
        "GRU:#{hostname}"
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
