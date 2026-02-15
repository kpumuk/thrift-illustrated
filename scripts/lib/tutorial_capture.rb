# frozen_string_literal: true

require "socket"
require "thread"
require "timeout"
require "thrift"

require_relative "recording_transport"

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

    OP_ADD = 1
    OP_SUBTRACT = 2
    OP_MULTIPLY = 3
    OP_DIVIDE = 4

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

      logs = {}
      CALL_SEQUENCE.each do
        name, message_type, seqid = protocol.read_message_begin
        response = handle_server_request(protocol, name: name, message_type: message_type, seqid: seqid, logs: logs)
        protocol.read_message_end

        next if message_type == Thrift::MessageTypes::ONEWAY

        write_server_response(protocol, response)
      end

      recording.flush
      state[:records] = recording.records
    ensure
      wrapped&.close
      accepted&.close
      server_transport&.close
    end

    def handle_server_request(protocol, name:, message_type:, seqid:, logs:)
      case name
      when "ping"
        read_ping_args(protocol)
        { name: name, seqid: seqid, result: :void }
      when "add"
        n1, n2 = read_add_args(protocol)
        { name: name, seqid: seqid, result: :i32, value: n1 + n2 }
      when "calculate"
        log_id, work = read_calculate_args(protocol)

        case work[:op]
        when OP_ADD
          value = work[:num1] + work[:num2]
        when OP_SUBTRACT
          value = work[:num1] - work[:num2]
        when OP_MULTIPLY
          value = work[:num1] * work[:num2]
        when OP_DIVIDE
          if work[:num2].zero?
            return {
              name: name,
              seqid: seqid,
              result: :exception,
              exception: {
                what_op: work[:op],
                why: "Cannot divide by 0"
              }
            }
          end

          value = work[:num1] / work[:num2]
        else
          return {
            name: name,
            seqid: seqid,
            result: :exception,
            exception: {
              what_op: work[:op],
              why: "Invalid operation"
            }
          }
        end

        logs[log_id] = value.to_s
        { name: name, seqid: seqid, result: :i32, value: value }
      when "getStruct"
        key = read_get_struct_args(protocol)
        {
          name: name,
          seqid: seqid,
          result: :shared_struct,
          value: {
            key: key,
            value: logs.fetch(key, "")
          }
        }
      when "zip"
        read_zip_args(protocol)
        { name: name, seqid: seqid, result: :void }
      else
        protocol.skip(Thrift::Types::STRUCT)
        {
          name: name,
          seqid: seqid,
          result: :application_exception,
          exception: {
            type: Thrift::ApplicationException::UNKNOWN_METHOD,
            message: "Unknown method #{name}"
          }
        }
      end
    end

    def write_server_response(protocol, response)
      if response[:result] == :application_exception
        protocol.write_message_begin(response[:name], Thrift::MessageTypes::EXCEPTION, response[:seqid])
        app_exception = Thrift::ApplicationException.new(response.dig(:exception, :message), response.dig(:exception, :type))
        app_exception.write(protocol)
        protocol.write_message_end
        protocol.trans.flush
        return
      end

      protocol.write_message_begin(response[:name], Thrift::MessageTypes::REPLY, response[:seqid])
      protocol.write_struct_begin("#{response[:name]}_result")

      case response[:result]
      when :void
        # no success field for void replies
      when :i32
        protocol.write_field_begin("success", Thrift::Types::I32, 0)
        protocol.write_i32(response[:value])
        protocol.write_field_end
      when :shared_struct
        protocol.write_field_begin("success", Thrift::Types::STRUCT, 0)
        protocol.write_struct_begin("SharedStruct")

        protocol.write_field_begin("key", Thrift::Types::I32, 1)
        protocol.write_i32(response.dig(:value, :key))
        protocol.write_field_end

        protocol.write_field_begin("value", Thrift::Types::STRING, 2)
        protocol.write_string(response.dig(:value, :value))
        protocol.write_field_end

        protocol.write_field_stop
        protocol.write_struct_end
        protocol.write_field_end
      when :exception
        protocol.write_field_begin("ouch", Thrift::Types::STRUCT, 1)
        protocol.write_struct_begin("InvalidOperation")

        protocol.write_field_begin("whatOp", Thrift::Types::I32, 1)
        protocol.write_i32(response.dig(:exception, :what_op))
        protocol.write_field_end

        protocol.write_field_begin("why", Thrift::Types::STRING, 2)
        protocol.write_string(response.dig(:exception, :why))
        protocol.write_field_end

        protocol.write_field_stop
        protocol.write_struct_end
        protocol.write_field_end
      end

      protocol.write_field_stop
      protocol.write_struct_end
      protocol.write_message_end
      protocol.trans.flush
    end

    def run_client
      socket = Thrift::Socket.new(@host, @port, @timeout_s)
      socket.open

      recording = ThriftIllustrated::RecordingTransport.new(inner_transport: socket, actor: "client")
      wrapped = wrap_transport(recording)
      protocol = build_protocol(wrapped)

      seqid = 1

      call_ping(protocol, seqid)
      seqid += 1

      call_add(protocol, seqid, 1, 1)
      seqid += 1

      call_add(protocol, seqid, 1, 4)
      seqid += 1

      call_calculate(protocol, seqid, 1, op: OP_SUBTRACT, num1: 15, num2: 10)
      seqid += 1

      call_get_struct(protocol, seqid, 1)
      seqid += 1

      call_calculate(protocol, seqid, 1, op: OP_DIVIDE, num1: 1, num2: 0, expect_exception: true)
      seqid += 1

      call_zip(protocol, seqid)

      {
        records: recording.records
      }
    ensure
      wrapped&.close
      socket&.close
    end

    def call_ping(protocol, seqid)
      protocol.write_message_begin("ping", Thrift::MessageTypes::CALL, seqid)
      protocol.write_struct_begin("ping_args")
      protocol.write_field_stop
      protocol.write_struct_end
      protocol.write_message_end
      protocol.trans.flush

      read_void_reply(protocol, "ping", seqid)
    end

    def call_add(protocol, seqid, num1, num2)
      protocol.write_message_begin("add", Thrift::MessageTypes::CALL, seqid)
      protocol.write_struct_begin("add_args")

      protocol.write_field_begin("num1", Thrift::Types::I32, 1)
      protocol.write_i32(num1)
      protocol.write_field_end

      protocol.write_field_begin("num2", Thrift::Types::I32, 2)
      protocol.write_i32(num2)
      protocol.write_field_end

      protocol.write_field_stop
      protocol.write_struct_end
      protocol.write_message_end
      protocol.trans.flush

      read_i32_reply(protocol, "add", seqid)
    end

    def call_calculate(protocol, seqid, log_id, op:, num1:, num2:, expect_exception: false)
      protocol.write_message_begin("calculate", Thrift::MessageTypes::CALL, seqid)
      protocol.write_struct_begin("calculate_args")

      protocol.write_field_begin("logid", Thrift::Types::I32, 1)
      protocol.write_i32(log_id)
      protocol.write_field_end

      protocol.write_field_begin("w", Thrift::Types::STRUCT, 2)
      protocol.write_struct_begin("Work")

      protocol.write_field_begin("num1", Thrift::Types::I32, 1)
      protocol.write_i32(num1)
      protocol.write_field_end

      protocol.write_field_begin("num2", Thrift::Types::I32, 2)
      protocol.write_i32(num2)
      protocol.write_field_end

      protocol.write_field_begin("op", Thrift::Types::I32, 3)
      protocol.write_i32(op)
      protocol.write_field_end

      protocol.write_field_stop
      protocol.write_struct_end
      protocol.write_field_end

      protocol.write_field_stop
      protocol.write_struct_end
      protocol.write_message_end
      protocol.trans.flush

      if expect_exception
        read_calculate_exception_reply(protocol, "calculate", seqid)
      else
        read_i32_reply(protocol, "calculate", seqid)
      end
    end

    def call_get_struct(protocol, seqid, key)
      protocol.write_message_begin("getStruct", Thrift::MessageTypes::CALL, seqid)
      protocol.write_struct_begin("getStruct_args")

      protocol.write_field_begin("key", Thrift::Types::I32, 1)
      protocol.write_i32(key)
      protocol.write_field_end

      protocol.write_field_stop
      protocol.write_struct_end
      protocol.write_message_end
      protocol.trans.flush

      read_shared_struct_reply(protocol, "getStruct", seqid)
    end

    def call_zip(protocol, seqid)
      protocol.write_message_begin("zip", Thrift::MessageTypes::ONEWAY, seqid)
      protocol.write_struct_begin("zip_args")
      protocol.write_field_stop
      protocol.write_struct_end
      protocol.write_message_end
      protocol.trans.flush
    end

    def read_void_reply(protocol, expected_name, expected_seqid)
      name, type, seqid = protocol.read_message_begin
      verify_reply_header!(name: name, type: type, seqid: seqid, expected_name: expected_name, expected_seqid: expected_seqid)

      protocol.read_struct_begin
      loop do
        _field_name, field_type, _field_id = protocol.read_field_begin
        break if field_type == Thrift::Types::STOP

        protocol.skip(field_type)
        protocol.read_field_end
      end
      protocol.read_struct_end
      protocol.read_message_end
    end

    def read_i32_reply(protocol, expected_name, expected_seqid)
      name, type, seqid = protocol.read_message_begin
      verify_reply_header!(name: name, type: type, seqid: seqid, expected_name: expected_name, expected_seqid: expected_seqid)

      value = nil

      protocol.read_struct_begin
      loop do
        _field_name, field_type, field_id = protocol.read_field_begin
        break if field_type == Thrift::Types::STOP

        if field_id == 0 && field_type == Thrift::Types::I32
          value = protocol.read_i32
        else
          protocol.skip(field_type)
        end

        protocol.read_field_end
      end
      protocol.read_struct_end
      protocol.read_message_end

      value
    end

    def read_shared_struct_reply(protocol, expected_name, expected_seqid)
      name, type, seqid = protocol.read_message_begin
      verify_reply_header!(name: name, type: type, seqid: seqid, expected_name: expected_name, expected_seqid: expected_seqid)

      struct_value = {}

      protocol.read_struct_begin
      loop do
        _field_name, field_type, field_id = protocol.read_field_begin
        break if field_type == Thrift::Types::STOP

        if field_id == 0 && field_type == Thrift::Types::STRUCT
          protocol.read_struct_begin
          loop do
            _inner_name, inner_type, inner_id = protocol.read_field_begin
            break if inner_type == Thrift::Types::STOP

            case inner_id
            when 1
              struct_value[:key] = protocol.read_i32
            when 2
              struct_value[:value] = protocol.read_string
            else
              protocol.skip(inner_type)
            end

            protocol.read_field_end
          end
          protocol.read_struct_end
        else
          protocol.skip(field_type)
        end

        protocol.read_field_end
      end
      protocol.read_struct_end
      protocol.read_message_end

      struct_value
    end

    def read_calculate_exception_reply(protocol, expected_name, expected_seqid)
      name, type, seqid = protocol.read_message_begin
      verify_reply_header!(name: name, type: type, seqid: seqid, expected_name: expected_name, expected_seqid: expected_seqid)

      exception = {}

      protocol.read_struct_begin
      loop do
        _field_name, field_type, field_id = protocol.read_field_begin
        break if field_type == Thrift::Types::STOP

        if field_id == 1 && field_type == Thrift::Types::STRUCT
          protocol.read_struct_begin
          loop do
            _inner_name, inner_type, inner_id = protocol.read_field_begin
            break if inner_type == Thrift::Types::STOP

            case inner_id
            when 1
              exception[:what_op] = protocol.read_i32
            when 2
              exception[:why] = protocol.read_string
            else
              protocol.skip(inner_type)
            end

            protocol.read_field_end
          end
          protocol.read_struct_end
        else
          protocol.skip(field_type)
        end

        protocol.read_field_end
      end
      protocol.read_struct_end
      protocol.read_message_end

      exception
    end

    def read_ping_args(protocol)
      protocol.read_struct_begin
      loop do
        _field_name, field_type, _field_id = protocol.read_field_begin
        break if field_type == Thrift::Types::STOP

        protocol.skip(field_type)
        protocol.read_field_end
      end
      protocol.read_struct_end
    end

    def read_add_args(protocol)
      values = { num1: 0, num2: 0 }
      protocol.read_struct_begin
      loop do
        _field_name, field_type, field_id = protocol.read_field_begin
        break if field_type == Thrift::Types::STOP

        if field_id == 1 && field_type == Thrift::Types::I32
          values[:num1] = protocol.read_i32
        elsif field_id == 2 && field_type == Thrift::Types::I32
          values[:num2] = protocol.read_i32
        else
          protocol.skip(field_type)
        end

        protocol.read_field_end
      end
      protocol.read_struct_end

      [values[:num1], values[:num2]]
    end

    def read_calculate_args(protocol)
      values = { log_id: 0, work: { op: OP_ADD, num1: 0, num2: 0 } }

      protocol.read_struct_begin
      loop do
        _field_name, field_type, field_id = protocol.read_field_begin
        break if field_type == Thrift::Types::STOP

        if field_id == 1 && field_type == Thrift::Types::I32
          values[:log_id] = protocol.read_i32
        elsif field_id == 2 && field_type == Thrift::Types::STRUCT
          values[:work] = read_work_struct(protocol)
        else
          protocol.skip(field_type)
        end

        protocol.read_field_end
      end
      protocol.read_struct_end

      [values[:log_id], values[:work]]
    end

    def read_work_struct(protocol)
      work = { op: OP_ADD, num1: 0, num2: 0 }

      protocol.read_struct_begin
      loop do
        _field_name, field_type, field_id = protocol.read_field_begin
        break if field_type == Thrift::Types::STOP

        case field_id
        when 1
          work[:num1] = protocol.read_i32
        when 2
          work[:num2] = protocol.read_i32
        when 3
          work[:op] = protocol.read_i32
        when 4
          protocol.read_string
        else
          protocol.skip(field_type)
        end

        protocol.read_field_end
      end
      protocol.read_struct_end

      work
    end

    def read_get_struct_args(protocol)
      key = 0
      protocol.read_struct_begin
      loop do
        _field_name, field_type, field_id = protocol.read_field_begin
        break if field_type == Thrift::Types::STOP

        if field_id == 1 && field_type == Thrift::Types::I32
          key = protocol.read_i32
        else
          protocol.skip(field_type)
        end

        protocol.read_field_end
      end
      protocol.read_struct_end

      key
    end

    def read_zip_args(protocol)
      read_ping_args(protocol)
    end

    def verify_reply_header!(name:, type:, seqid:, expected_name:, expected_seqid:)
      return if name == expected_name && type == Thrift::MessageTypes::REPLY && seqid == expected_seqid

      raise "Unexpected reply header: name=#{name.inspect} type=#{type.inspect} seqid=#{seqid.inspect}"
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
