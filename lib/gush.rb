require "bundler/setup"

require "graphviz"
require "hiredis"
require "pathname"
require "redis"
require "securerandom"
require "sidekiq"

require "gush/json"
require "gush/cli"
require "gush/cli/overview"
require "gush/graph"
require "gush/client"
require "gush/configuration"
require "gush/errors"
require "gush/job"
require "gush/worker"
require "gush/workflow"

module Gush
  def self.endpoint
    configuration.endpoint
  end

  def self.root
    Pathname.new(__FILE__).parent.parent
  end

  def self.configuration
    @configuration ||= Configuration.new
  end

  def self.configure
    yield configuration
    reconfigure_sidekiq
  end

  def self.reconfigure_sidekiq
    Sidekiq.configure_server do |config|
      config.redis = { url: configuration.redis_url, queue: configuration.namespace}
    end

    Sidekiq.configure_client do |config|
      config.redis = { url: configuration.redis_url, queue: configuration.namespace}
    end
  end
end

Gush.reconfigure_sidekiq
