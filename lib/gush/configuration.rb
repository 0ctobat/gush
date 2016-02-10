require 'yajl'

module Gush
  class Configuration
    attr_accessor :concurrency, :namespace, :redis_url, :environment

    def self.from_json(json)
      new(Gush::JSON.decode(json, symbolize_keys: true))
    end

    def initialize(hash = {})
      self.concurrency = hash.fetch(:concurrency, 5)
      self.namespace   = hash.fetch(:namespace, 'gush')
      self.redis_url   = hash.fetch(:redis_url, 'redis://localhost:6379')
      self.endpoint    = hash.fetch(:endpoint, '.')
      self.environment = hash.fetch(:environment, 'development')
    end

    def endpoint=(path)
      @endpoint = Pathname(path)
    end

    def endpoint
      @endpoint.realpath
    end

    def to_hash
      {
        concurrency: concurrency,
        namespace:   namespace,
        redis_url:   redis_url,
        environment: environment
      }
    end

    def to_json
      Gush::JSON.encode(to_hash)
    end
  end
end
