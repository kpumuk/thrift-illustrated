# frozen_string_literal: true

require "minitest/autorun"
require "thrift"
require_relative "../scripts/lib/message_parser"

class MessageParserTest < Minitest::Test
  class BufferWriter < Thrift::BaseTransport
    attr_reader :buffer

    def initialize
      @buffer = +"".b
    end

    def write(buf)
      @buffer << buf.to_s.b
    end
  end

  def test_parses_binary_buffered_message
    payload = build_message(protocol: "binary", method: "ping", seqid: 7, fields: [
      {id: 1, type: Thrift::Types::I32, value: 42},
      {id: 2, type: Thrift::Types::STRING, value: "hello"}
    ])

    parser = ThriftIllustrated::MessageParser.new
    result = parser.parse_bytes(raw_bytes: payload, protocol: "binary", transport: "buffered", actor: "client")

    assert_equal 1, result.fetch(:messages).length
    message = result.fetch(:messages).first

    assert_equal "client", message.fetch(:actor)
    assert_equal "client->server", message.fetch(:direction)
    assert_equal "ping", message.fetch(:method)
    assert_equal "call", message.fetch(:message_type)
    assert_equal 7, message.fetch(:seqid)
    assert_equal "buffered", message.fetch(:transport).fetch(:type)
    assert_nil message.fetch(:transport).fetch(:frame_length)
    assert_equal payload.bytesize, message.fetch(:raw_size)

    field_ids = message.fetch(:payload).fetch(:fields).map { |field| field.fetch(:id) }
    assert_equal [1, 2], field_ids
    assert_empty message.fetch(:parse_errors)
  end

  def test_parses_compact_framed_messages
    payload_one = build_message(protocol: "compact", method: "add", seqid: 1, fields: [{id: 1, type: Thrift::Types::I32, value: 2}])
    payload_two = build_message(protocol: "compact", method: "zip", seqid: 2, message_type: Thrift::MessageTypes::ONEWAY, fields: [])

    framed = frame(payload_one) + frame(payload_two)

    parser = ThriftIllustrated::MessageParser.new
    result = parser.parse_bytes(raw_bytes: framed, protocol: "compact", transport: "framed", actor: "server")

    assert_equal 2, result.fetch(:messages).length

    first = result.fetch(:messages).first
    second = result.fetch(:messages).last

    assert_equal "server", first.fetch(:actor)
    assert_equal "server->client", first.fetch(:direction)
    assert_equal "framed", first.fetch(:transport).fetch(:type)
    assert_equal payload_one.bytesize, first.fetch(:transport).fetch(:frame_length)
    assert_equal "add", first.fetch(:method)

    assert_equal "oneway", second.fetch(:message_type)
    assert_equal "zip", second.fetch(:method)
    assert_empty first.fetch(:parse_errors)
    assert_empty second.fetch(:parse_errors)
  end

  def test_reports_frame_truncated
    payload = build_message(protocol: "binary", method: "ping", seqid: 1, fields: [])
    raw = [payload.bytesize + 8].pack("N") + payload.byteslice(0, 2)

    parser = ThriftIllustrated::MessageParser.new
    result = parser.parse_bytes(raw_bytes: raw, protocol: "binary", transport: "framed", actor: "client")

    assert_equal 1, result.fetch(:messages).length
    parse_errors = result.fetch(:messages).first.fetch(:parse_errors)
    assert_equal "E_FRAME_TRUNCATED", parse_errors.first.fetch(:code)
  end

  def test_enforces_field_node_limit
    payload = build_message(protocol: "binary", method: "ping", seqid: 1, fields: [
      {id: 1, type: Thrift::Types::I32, value: 1},
      {id: 2, type: Thrift::Types::I32, value: 2}
    ])

    parser = ThriftIllustrated::MessageParser.new(max_field_nodes: 1)
    result = parser.parse_bytes(raw_bytes: payload, protocol: "binary", transport: "buffered", actor: "client")

    error_codes = result.fetch(:messages).first.fetch(:parse_errors).map { |error| error.fetch(:code) }
    assert_includes error_codes, "E_FIELD_NODE_LIMIT"
  end

  private

  def build_message(protocol:, method:, seqid:, fields:, message_type: Thrift::MessageTypes::CALL)
    transport = BufferWriter.new
    proto = (protocol == "binary") ? Thrift::BinaryProtocol.new(transport) : Thrift::CompactProtocol.new(transport)

    proto.write_message_begin(method, message_type, seqid)
    proto.write_struct_begin("args")

    fields.each do |field|
      proto.write_field_begin("", field.fetch(:type), field.fetch(:id))
      write_field_value(proto, field.fetch(:type), field.fetch(:value))
      proto.write_field_end
    end

    proto.write_field_stop
    proto.write_struct_end
    proto.write_message_end

    transport.buffer
  end

  def write_field_value(protocol, type, value)
    case type
    when Thrift::Types::BOOL
      protocol.write_bool(value)
    when Thrift::Types::BYTE
      protocol.write_byte(value)
    when Thrift::Types::I16
      protocol.write_i16(value)
    when Thrift::Types::I32
      protocol.write_i32(value)
    when Thrift::Types::I64
      protocol.write_i64(value)
    when Thrift::Types::DOUBLE
      protocol.write_double(value)
    when Thrift::Types::STRING
      protocol.write_string(value)
    else
      raise ArgumentError, "Unsupported test field type #{type}"
    end
  end

  def frame(payload)
    [payload.bytesize].pack("N") + payload
  end
end
