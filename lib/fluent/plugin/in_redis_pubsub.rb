module Fluent
  class RedisPubsubInput < Input

    Plugin.register_input('redis_pubsub', self)

    config_param :channel, :string
    config_param :tag,     :string

    def initialize
      require 'redis'
      super
    end

    def configure(config)
      super
      @channel = config['channel'].to_s
      @tag     = config['tag'].to_s
    end

    def start
      super
      @thread = Thread.new(&method(:run))
    end

    def run
      @redis = Redis.new(host: ENV['REDIS_HOST'], port: ENV['REDIS_PORT'], password: ENV['REDIS_PASSWORD'])
      @redis.subscribe(@channel) do |on|
        on.subscribe do |channel, subscriptions|
          log.debug "Subscribed to #{ channel }"
        end

        on.message do |channel, message|
          begin
            parsed = JSON.parse(message)
            log.debug parsed
          rescue JSON::ParserError => e
            log.error e
          end
          Fluent::Engine.emit((@tag || channel), Engine.now, (parsed || message))
        end
      end
    end

    def shutdown
      Thread.kill(@thread)
      @redis.quit
    end

  end
end
