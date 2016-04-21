require 'socket'

module Gru
  module Adapters
    class RedisAdapter
      attr_reader :client

      def initialize(settings)
        @settings = settings
        @client = initialize_client(settings.client_settings)
      end

      def set_worker_counts
        set_rebalance_flag(@settings.rebalance_flag)
        set_presume_host_dead_after(@settings.presume_host_dead_after)
        release_workers
        register_workers(@settings.host_maximums)
        set_max_worker_counts(@settings.host_maximums)
        register_global_workers(@settings.cluster_maximums)
        set_max_global_worker_counts(@settings.cluster_maximums)
        update_heartbeat if manage_heartbeat?
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
          host_count = local_running_count(worker)
          global_count = host_count
          host_count.times do
            global_count = send_message(:hincrby, global_workers_running_key, worker, -1) if global_count > 0
            host_count = send_message(:hincrby, host_workers_running_key,worker,-1) if host_count > 0
          end
        end
        send_message(:del, host_workers_running_key)
        send_message(:del, host_max_worker_key)
        send_message(:hdel, heartbeat_key, hostname)
      end

      def release_presumed_dead_worker_hosts
        return false unless manage_heartbeat?
        update_heartbeat
        presumed_dead_worker_hosts.each_pair do |hostname,timestamp|
          lock_key = "#{gru_key}:removing_dead_host:#{hostname}"
          if send_message(:setnx,lock_key,Time.now.to_i)
            remove_worker_host(hostname)
            send_message(:hdel,heartbeat_key,hostname)
            send_message(:del,lock_key)
            return true
          end
        end
        false
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

      def manage_heartbeat?
        @settings.manage_worker_heartbeats
      end

      def update_heartbeat
        send_message(:hset,heartbeat_key,hostname,Time.now.to_i)
      end

      def remove_worker_host(hostname)
        workers = send_message(:hgetall, "#{gru_key}:#{hostname}:workers_running")
        workers.each_pair do |worker_name, count|
          local_count, global_count = Integer(count), Integer(count)
          Integer(count).times do
            local_count = send_message(:hincrby,"#{gru_key}:#{hostname}:workers_running",worker_name,-1) if local_count > 0
            global_count = send_message(:hincrby,global_workers_running_key,worker_name,-1) if global_count > 0
          end
        end
        send_message(:del,"#{gru_key}:#{hostname}:workers_running")
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

      def workers_with_heartbeats
        send_message(:hgetall, heartbeat_key)
      end

      def reserve_worker?(worker)
        host_running,global_running,host_max,global_max = worker_counts(worker)
        result = false
        if rebalance_cluster?
          result = host_running.to_i < max_workers_per_host(global_max,host_max) &&
            host_running.to_i < @settings.max_worker_processes_per_host
        else
          result = host_running.to_i < host_max.to_i
        end
          result && global_running.to_i < global_max.to_i
      end

      def expire_worker?(worker)
        host_running,global_running,host_max,global_max = worker_counts(worker)
        result = false
        if rebalance_cluster?
          result = host_running.to_i > max_workers_per_host(global_max,host_max) ||
            host_running.to_i > @settings.max_worker_processes_per_host
        else
          result = host_running.to_i > host_max.to_i
        end
         (result || global_running.to_i > global_max.to_i) && host_running.to_i >= 0
      end

      def worker_counts(worker)
        counts = @client.multi do |multi|
          multi.hget(host_workers_running_key,worker)
          multi.hget(global_workers_running_key,worker)
          multi.hget(host_max_worker_key,worker)
          multi.hget(global_max_worker_key,worker)
        end
        if counts[1].to_i <0
          make_global_workers_count_non_negative(worker)
          counts[1] = send_message(:hget, global_workers_running_key, worker)
        end
        counts
      end

      def presumed_dead_worker_hosts
        workers_with_heartbeats.select{ |hostname, timestamp| timestamp.to_i + presume_host_dead_after < Time.now.to_i}
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

      def make_global_workers_count_non_negative(worker)
        send_message(:hset, global_workers_running_key, worker, 0)
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

      def heartbeat_key
        "#{gru_key}:heartbeats"
      end

      def gru_key
        "GRU:#{@settings.environment_name}:#{@settings.cluster_name}"
      end

      def hostname
        @hostname ||= Socket.gethostname
      end

      def send_message(action,*args)
        @client.send(action,*args)
      end

      def initialize_client(config=nil)
        Redis.new(config || {})
      end
    end
  end
end
