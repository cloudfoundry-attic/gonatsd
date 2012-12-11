# Copyright (c) 2009-2012 VMware, Inc.

module PubsubApp

  class << self
    attr_accessor :config
    attr_accessor :logger
    attr_accessor :nats_uri

    # @return [Array<PubRequest>]
    attr_accessor :pubs

    # @return [Array<SubRequest>]
    attr_accessor :subs

    # @param [Hash] config
    def config=(config)
      @config = config

      validate_config
      setup_logging

      @nats_uri = @config["nats_uri"]

      @pubs = (@config["pubs"] || []).map do |pub|
        PubsubApp::PubRequest.from_hash(pub)
      end

      @subs = (@config["subs"] || []).map do |sub|
        PubsubApp::SubRequest.from_hash(sub)
      end
    end

    def validate_config
      unless @config.is_a?(Hash)
        raise ConfigError, "Invalid config format, Hash expected, " +
          "#{@config.class} given"
      end

      if @config["nats_uri"].nil?
        raise ConfigError, "NATS URI is missing"
      end

      if @config["pubs"] && !@config["pubs"].is_a?(Array)
        raise ConfigError, "'pubs' should be an Array"
      end

      if @config["subs"] && !@config["subs"].is_a?(Array)
        raise ConfigError, "'subs' should be an Array"
      end
    end

    def setup_logging
      @logger = Logger.new(@config["logfile"] || STDOUT)

      case @config["loglevel"].to_s.downcase
      when "fatal"
        @logger.level = Logger::FATAL
      when "error"
        @logger.level = Logger::ERROR
      when "warn"
        @logger.level = Logger::WARN
      when "info"
        @logger.level = Logger::INFO
      when "", "debug"
        @logger.level = Logger::DEBUG
      else
        raise ConfigError, "Unknown log level '#{@config["loglevel"]}'"
      end
    end

  end
end