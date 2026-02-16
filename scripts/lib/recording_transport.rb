# frozen_string_literal: true

require "base64"

module ThriftIllustrated
  class RecordingTransport
    VALID_ACTORS = %w[client server].freeze

    def initialize(inner_transport:, actor:)
      @inner_transport = inner_transport
      @actor = actor.to_s
      validate_actor!
      validate_inner_transport!

      @records = []
      @chunk_index = 0
      @lock = Mutex.new
    end

    attr_reader :actor

    def write(data)
      payload = normalize_payload(data)
      result = @inner_transport.write(payload)
      record_write(payload) unless payload.empty?
      result
    end

    def read(size)
      @inner_transport.read(size)
    end

    def flush
      @inner_transport.flush
    end

    def close
      @inner_transport.close
    end

    def records
      @lock.synchronize { @records.dup }
    end

    def method_missing(method_name, *args, &block)
      return super unless @inner_transport.respond_to?(method_name)

      @inner_transport.public_send(method_name, *args, &block)
    end

    def respond_to_missing?(method_name, include_private = false)
      @inner_transport.respond_to?(method_name, include_private) || super
    end

    private

    def validate_actor!
      return if VALID_ACTORS.include?(@actor)

      raise ArgumentError, "actor must be one of: #{VALID_ACTORS.join(", ")}" \
        " (got #{@actor.inspect})"
    end

    def validate_inner_transport!
      %i[write read flush close].each do |required|
        next if @inner_transport.respond_to?(required)

        raise ArgumentError, "inner_transport must respond to ##{required}"
      end
    end

    def normalize_payload(data)
      payload = if data.respond_to?(:to_str)
        data.to_str
      else
        raise TypeError, "write expects a String-compatible payload"
      end

      payload.b
    end

    def record_write(payload)
      record = {
        chunk_index: nil,
        actor: @actor,
        bytes_base64: Base64.strict_encode64(payload),
        byte_len: payload.bytesize,
        monotonic_ns: Process.clock_gettime(Process::CLOCK_MONOTONIC, :nanosecond)
      }

      @lock.synchronize do
        record[:chunk_index] = @chunk_index
        @chunk_index += 1
        @records << record.freeze
      end
    end
  end
end
