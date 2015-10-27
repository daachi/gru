require 'socket'

module Gru
  module Adapters
    class RedisAdapter
      attr_reader :client

      def initialize(client,settings)
        @client = client
        @settings = settings
      end

      def set_worker_counts
        set_rebalance_flag(@settings.rebalance_flag)
        set_presume_host_dead_after(@settings.presume_host_dead_after)
        release_workers
        register_workers(@settings.host_maximums)
        set_max_worker_counts(@settings.host_maximums)
        register_global_workers(@settings.cluster_maximums)
        set_max_global_worker_counts(@settings.cluster_maximums)
      end

      def provision_workers
        available = {}
        workers = max_host_workers
        workers.each do |worker, count|
          available[worker] = with_worker_counts(worker,count) do |total|
            if reserve_worker?(worker)
              total += 1 if reserve_worker(worker)
            end
            total
          end
        end
        available
      end

      def expire_workers
        removable = {}
        workers = max_host_workers
        workers.each do |worker, count|
          removable[worker] = with_worker_counts(worker,count) do |total|
            if expire_worker?(worker)
              total -= 1 if expire_worker(worker)
            end
            total
          end
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

      def release_presumed_dead_workers
        presumed_dead_cluster_members.each_pair do |hostname,timestamp|
          lock_key = "#{gru_key}:removing_dead_host:#{hostname}"
          if send_message(:setnx,lock_key,Time.now.to_i)
            remove_dead_host_workers_from_counts(hostname)
            send_message(:del,lock_key)
            return true
          end
          false
        end
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

      def set_rebalance_flag(rebalance)
        send_message(:set,"#{gru_key}:rebalance",rebalance)
      end

      def set_presume_host_dead_after(seconds)
        send_message(:set,"#{gru_key}:presume_host_dead_after",seconds)
      end

      def register_worker(worker,count)
        send_message(:hsetnx,host_workers_running_key,worker,count)
      end

      def register_global_worker(worker,count)
        send_message(:hsetnx,global_workers_running_key,worker,count)
      end

      def set_max_worker_count(worker,count)
        send_message(:hset,host_max_worker_key,worker,count)
      end

      def set_max_global_worker_count(worker,count)
        send_message(:hset,global_max_worker_key,worker,count)
      end

      def remove_dead_host_workers_from_counts(hostname)
        workers_running_on_dead_host = send_message(:hgetall, "#{gru_key}:#{hostname}:workers_running")
        workers_running_on_dead_host.each_pair do |worker_name, count|
          send_message(:hincrby,"#{gru_key}:#{hostname}:workers_running",worker_name,Integer(count)*-1)
          send_message(:hincrby,global_workers_running_key,worker_name,Integer(count)*-1)
        end
        send_message(:hdel,resque_cluster_pings_key,hostname)
      end

      def reset_removed_global_worker_counts(workers)
        global_max = max_host_workers
        global_max.each_pair do |worker, count|
          set_max_global_worker_count(worker,0) unless workers[worker]
        end
      end

      def with_worker_counts(worker,count,&block)
        Integer(count).times.reduce(0) do |total|
          block.call(total)
        end
      end

      def reserve_worker(worker)
        adjust_workers(worker,1)
      end

      def expire_worker(worker)
        adjust_workers(worker,-1)
      end

      def adjust_workers(worker,amount)
        lock_key = "#{gru_key}:#{worker}"
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

      def max_global_workers
        send_message(:hgetall,global_max_worker_key)
      end

      def resque_cluster_members
        send_message(:hgetall, resque_cluster_pings_key)
      end

      def reserve_worker?(worker)
        host_running,global_running,host_max,global_max = worker_counts(worker)
        result = false
        if rebalance_cluster?
          result = host_running.to_i < max_workers_per_host(global_max,host_max)
        else
          result = host_running.to_i < host_max.to_i
        end
          result && global_running.to_i < global_max.to_i
      end

      def expire_worker?(worker)
        host_running,global_running,host_max,global_max = worker_counts(worker)
        result = false
        if rebalance_cluster?
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

      def presumed_dead_cluster_members
        resque_cluster_members.select{ |hostname, timestamp| Time.parse(timestamp).to_i + presume_host_dead_after < Time.now.to_i}
      end

      def local_running_count(worker)
        send_message(:hget,host_workers_running_key,worker).to_i
      end

      def gru_host_count
        send_message(:keys,"#{gru_key}:*:workers_running").count - 1
      end

      def max_workers_per_host(global_worker_max_count,host_max)
        host_count = gru_host_count
        rebalance_count = host_count > 0 ? (global_worker_max_count.to_i/host_count.to_f).ceil : host_max.to_i
        rebalance_count <= host_max.to_i && host_count > 1 ? rebalance_count : host_max.to_i
      end

      def rebalance_cluster?
        send_message(:get,"#{gru_key}:rebalance") == "true"
      end

      def presume_host_dead_after
        dead_after_number_of_seconds = send_message(:get,"#{gru_key}:presume_host_dead_after").to_i
        dead_after_number_of_seconds > 0 ? dead_after_number_of_seconds : 120
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
        "#{gru_key}:global"
      end

      def host_key
        "#{gru_key}:#{hostname}"
      end

      def gru_key
        "GRU:#{@settings.environment_name}:#{@settings.cluster_name}"
      end

      def resque_cluster_pings_key
        "resque:cluster:#{@settings.cluster_name}:#{@settings.environment_name}:pings"
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
