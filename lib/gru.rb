require 'gru/version'
require 'gru/worker_manager'
require 'gru/hooks'
require 'gru/configuration'

module Gru
  def self.create(settings)
    configuration = Gru::Configuration.new(settings)
    hooks = Gru::Hooks::CustomHooks.constants.map {|x| Gru::Hooks::CustomHooks.const_get(x) }
    hooks.unshift(Gru::Hooks::DefaultHook)
    manager = WorkerManager.new(configuration.adapter, hooks)
    manager.register_workers
    manager
  end
end
