# Copyright (c) 2009-2012 VMware, Inc.

module PubsubApp
  class Runner

    # @param [Hash] config App configuration
    def self.run(config)
      new(config).run
    end

    # @param [Hash] config App configuration
    def initialize(config)
      PubsubApp.config = YAML.load_file(config)

      @nats_uri = PubsubApp.nats_uri
      @logger = PubsubApp.logger
      @pubs = PubsubApp.pubs
      @subs = PubsubApp.subs

      @shutting_down = false
      @nats = nil
    end

    def run
      @logger.info("Starting PubsubApp...")
      @logger.info("pubs: #{@pubs.size}, subs: #{@subs.size}")

      EM.kqueue if EM.kqueue?
      EM.epoll if EM.epoll?

      EM.error_handler { |e| handle_em_error(e) }

      EM.run do
        connect_to_nats
      end
    end

    def stop
      unless @shutting_down
        @shutting_down = true
        @logger.info("PubsubApp shutting down...")
        EM.stop
      end
    end

    def connect_to_nats
      NATS.on_error do |e|
        log_exception(e)
        stop if e.kind_of?(NATS::ConnectError)
      end

      nats_client_options = {
        :uri => @nats_uri,
        :autostart => false
      }

      @logger.info("Connecting to NATS at '#{@nats_uri}'...")
      @nats = NATS.connect(nats_client_options) do
        @logger.info("Connected to NATS at '#{@nats_uri}'")
        setup_pubsub
      end
    end

    def setup_pubsub
      @pubs.each do |pub|
        pub.schedule(@nats)
      end

      @subs.each do |sub|
        sub.execute(@nats)
      end
    end

    def handle_em_error(e)
      log_exception(e, :fatal)
      stop unless @shutting_down
    end

    def log_exception(e, level = :error)
      level = :error unless level == :fatal
      @logger.send(level, e.to_s)
      if e.respond_to?(:backtrace) && e.backtrace.respond_to?(:join)
        @logger.send(level, e.backtrace.join("\n"))
      end
    end

  end
end