module Gru
  module Hooks
    class DefaultHook
      def initialize(adapter, results = {})
        @results = results
        @adapter = adapter
      end

      def call
        results = @results
        add = @adapter.provision_workers
        remove = @adapter.expire_workers
        keys = add.keys + remove.keys
        keys.uniq.each do |key|
          results[key] = add.fetch(key) {0} + remove.fetch(key) {0}
        end
        results
      end
    end

    module CustomHooks; end
  end
end
