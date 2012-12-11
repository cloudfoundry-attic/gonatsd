# Copyright (c) 2009-2012 VMware, Inc.

module PubsubApp

  class PubRequest

    DISTRIBUTIONS = %w(normal random)

    def self.from_hash(pub)
      new(pub["subject"], pub["interval"], pub["payload"])
    end

    def initialize(subject, interval, payload)
      @logger = PubsubApp.logger

      @subject = subject
      @interval = interval
      @payload = payload

      validate_distribution(@interval)
      validate_distribution(@payload)
    end

    def validate_distribution(dist)
      unless dist.is_a?(Array)
        raise Error, "Distribution should be an array"
      end

      kind = dist[0]
      available_kinds = DISTRIBUTIONS.join(", ")

      unless kind.is_a?(String)
        raise Error, "Distribution kind should be a String"
      end

      case kind
      when "normal"
        mean, variance = dist[1], dist[2]
        unless mean.is_a?(Numeric) && variance.is_a?(Numeric)
          raise Error, "Normal distribution should have " +
            "[normal, {mean}, {variance}] format"
        end
      when "random"
        min, max = dist[1], dist[2]
        unless min.is_a?(Numeric) && max.is_a?(Numeric)
          raise Error, "Random distribution should have " +
            "[random, {min}, {max}] format"
        end
        if min > max
          raise Error, "Min cannot be greater than max for random distribution"
        end
      else
        raise Error, "Supported distributions are: #{available_kinds}"
      end
    end

    def schedule(nats)
      publish(nats)
      interval = pick_interval
      @logger.debug("Next publish in #{interval}ms")
      EM.add_timer(interval / 1000.0) { schedule(nats) }
    end

    def publish(nats)
      payload = pick_payload
      subject = pick_subject

      nats.publish(subject, payload) do
        @logger.debug("Published [#{subject}]: #{payload.size} bytes")
      end
    end

    def pick_subject
      @subject.gsub("{guid}", SecureRandom.uuid)
    end

    # @return [Fixnum] Interval to wait for next pub (ms)
    def pick_interval
      pick(@interval)
    end

    def pick_payload
      size = pick(@payload)
      "a" * size
    end

    # @param [Array] dist Distribution
    # @return [Fixnum] Number picked according to given distribution
    def pick(dist)
      kind = dist[0]

      case kind
      when "normal"
        mean, variance = dist[1], dist[2]
        # Using Box-Muller:
        base = Math.sqrt(-2 * Math.log(rand)) * Math.cos(2 * Math::PI * rand)
        return [(mean + base * variance).round, 1].max
      when "random"
        min, max = dist[1], dist[2]
        return [min + ((max - min) * rand).round, 1].max
      else
        raise Error, "Unknown distribution '#{kind}'"
      end
    end

  end

end