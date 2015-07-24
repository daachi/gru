require 'gru/version'
require 'gru/worker_manager'
require 'gru/configuration'

module Gru
  def self.create(settings)
    configuration = Gru::Configuration.new(settings)
    manager = WorkerManager.new(configuration.adapter)
    manager.register_workers
    manager
  end
end
