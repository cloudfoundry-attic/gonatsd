# Copyright (c) 2009-2012 VMware, Inc.

module PubsubApp

  class SubRequest

    def self.from_hash(sub)
      new(sub["subject"])
    end

    def initialize(subject)
      if !subject.is_a?(String) || subject.to_s.empty?
        raise Error, "Invalid subscription, " +
          "subject should be a non-empty string"
      end

      @subject = subject.to_s
      @logger = PubsubApp.logger
    end

    def execute(nats)
      nats.subscribe(@subject) do |message, _, subject|
        @logger.debug("Received [#{subject}]: #{message.size} bytes")
      end
    end

  end

end