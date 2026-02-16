# frozen_string_literal: true

require "socket"
require "thread"
require "timeout"
require "thrift"

require_relative "recording_transport"
require_relative "thrift_client_seqid_patch"

module ThriftIllustrated
  class TutorialCapture
    CALL_SEQUENCE = [
      :ping,
      :add,
      :add,
      :calculate_subtract,
      :get_struct,
      :calculate_divide_by_zero,
      :zip
    ].freeze

    GENERATED_STUB_DIR = File.expand_path("../../thrift/gen-rb", __dir__)

    unless File.file?(File.join(GENERATED_STUB_DIR, "calculator.rb"))
      raise LoadError, "Generated Thrift stubs are missing at #{GENERATED_STUB_DIR}. Run `mise run gen`."
    end

    $LOAD_PATH.unshift(GENERATED_STUB_DIR) unless $LOAD_PATH.include?(GENERATED_STUB_DIR)
    require "calculator"

    class TutorialHandler
      def initialize
        @log = {}
      end

      def ping; end

      def add(n1, n2)
        n1 + n2
      end

      def calculate(logid, work)
        value = case work.op
                when Operation::ADD
                  work.num1 + work.num2
                when Operation::SUBTRACT
                  work.num1 - work.num2
                when Operation::MULTIPLY
                  work.num1 * work.num2
                when Operation::DIVIDE
                  raise InvalidOperation.new(whatOp: work.op, why: "Cannot divide by 0") if work.num2.zero?

                  work.num1 / work.num2
                else
                  raise InvalidOperation.new(whatOp: work.op, why: "Invalid operation")
                end

        @log[logid] = SharedStruct.new(key: logid, value: value.to_s)
        value
      end

      def getStruct(key)
        @log.fetch(key) { SharedStruct.new(key: key, value: "") }
      end

      def zip; end
    end

    def initialize(protocol:, transport:, host:, port:, timeout_ms:)
      @protocol = protocol
      @transport = transport
      @host = host
      @port = port
      @timeout_s = timeout_ms.to_f / 1000.0
    end

    def run
      server_state = {}
      server_ready = Queue.new

      server_thread = Thread.new do
        Thread.current.report_on_exception = false
        run_server(server_ready, server_state)
      end

      server_ready.pop
      client_state = run_client

      server_thread.join

      {
        client_records: Array(client_state[:records]),
        server_records: Array(server_state[:records])
      }
    ensure
      if server_thread&.alive?
        server_thread.kill
        server_thread.join
      end
    end

    private

    def run_server(server_ready, state)
      server_transport = Thrift::ServerSocket.new(@host, @port)
      server_transport.listen
      server_ready << true

      accepted = Timeout.timeout(@timeout_s) { server_transport.accept }
      recording = ThriftIllustrated::RecordingTransport.new(inner_transport: accepted, actor: "server")
      wrapped = wrap_transport(recording)
      protocol = build_protocol(wrapped)
      processor = Calculator::Processor.new(TutorialHandler.new)

      CALL_SEQUENCE.each do
        processed = processor.process(protocol, protocol)
        raise "Unexpected unknown method received by server processor" unless processed
      end

      recording.flush
      state[:records] = recording.records
    ensure
      wrapped&.close
      accepted&.close
      server_transport&.close
    end

    def run_client
      socket = Thrift::Socket.new(@host, @port, @timeout_s)
      socket.open

      recording = ThriftIllustrated::RecordingTransport.new(inner_transport: socket, actor: "client")
      wrapped = wrap_transport(recording)
      protocol = build_protocol(wrapped)
      client = Calculator::Client.new(protocol)

      client.ping

      raise "Unexpected add(1, 1) response" unless client.add(1, 1) == 2
      raise "Unexpected add(1, 4) response" unless client.add(1, 4) == 5

      subtract = Work.new(op: Operation::SUBTRACT, num1: 15, num2: 10)
      raise "Unexpected calculate subtract response" unless client.calculate(1, subtract) == 5

      shared = client.getStruct(1)
      unless shared.is_a?(SharedStruct) && shared.key == 1 && shared.value == "5"
        raise "Unexpected getStruct response"
      end

      divide = Work.new(op: Operation::DIVIDE, num1: 1, num2: 0)
      begin
        client.calculate(1, divide)
        raise "Expected InvalidOperation for divide-by-zero"
      rescue InvalidOperation => error
        unless error.whatOp == Operation::DIVIDE && error.why == "Cannot divide by 0"
          raise "Unexpected InvalidOperation payload"
        end
      end

      client.zip

      {
        records: recording.records
      }
    ensure
      wrapped&.close
      socket&.close
    end

    def wrap_transport(base_transport)
      case @transport
      when "buffered"
        Thrift::BufferedTransport.new(base_transport)
      when "framed"
        Thrift::FramedTransport.new(base_transport)
      else
        raise ArgumentError, "Unsupported transport #{@transport.inspect}"
      end
    end

    def build_protocol(transport)
      case @protocol
      when "binary"
        Thrift::BinaryProtocol.new(transport)
      when "compact"
        Thrift::CompactProtocol.new(transport)
      when "json"
        Thrift::JsonProtocol.new(transport)
      else
        raise ArgumentError, "Unsupported protocol #{@protocol.inspect}"
      end
    end
  end
end
