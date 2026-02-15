# frozen_string_literal: true

require "minitest/autorun"
require "base64"
require_relative "../scripts/lib/recording_transport"

class RecordingTransportTest < Minitest::Test
  class FakeTransport
    attr_reader :writes, :reads, :flushed, :closed

    def initialize
      @writes = []
      @reads = []
      @flushed = 0
      @closed = 0
    end

    def write(data)
      @writes << data
      data.bytesize
    end

    def read(size)
      @reads << size
      "x" * size
    end

    def flush
      @flushed += 1
      true
    end

    def close
      @closed += 1
      true
    end

    def open?
      true
    end
  end

  def test_records_write_chunks_with_expected_shape
    inner = FakeTransport.new
    transport = ThriftIllustrated::RecordingTransport.new(inner_transport: inner, actor: :client)

    transport.write("abc")
    transport.write("def")

    records = transport.records
    assert_equal 2, records.length

    first = records.fetch(0)
    second = records.fetch(1)

    assert_equal 0, first.fetch(:chunk_index)
    assert_equal 1, second.fetch(:chunk_index)
    assert_equal "client", first.fetch(:actor)
    assert_equal Base64.strict_encode64("abc"), first.fetch(:bytes_base64)
    assert_equal 3, first.fetch(:byte_len)
    assert_operator first.fetch(:monotonic_ns), :>=, 0
  end

  def test_empty_write_is_forwarded_but_not_recorded
    inner = FakeTransport.new
    transport = ThriftIllustrated::RecordingTransport.new(inner_transport: inner, actor: "server")

    transport.write("")

    assert_equal [""], inner.writes
    assert_equal [], transport.records
  end

  def test_read_flush_and_close_are_passthrough
    inner = FakeTransport.new
    transport = ThriftIllustrated::RecordingTransport.new(inner_transport: inner, actor: "client")

    assert_equal "xxxxx", transport.read(5)
    assert_equal [5], inner.reads

    assert transport.flush
    assert_equal 1, inner.flushed

    assert transport.close
    assert_equal 1, inner.closed

    assert_equal [], transport.records
  end

  def test_delegates_unknown_methods_to_inner_transport
    inner = FakeTransport.new
    transport = ThriftIllustrated::RecordingTransport.new(inner_transport: inner, actor: "client")

    assert transport.open?
    assert transport.respond_to?(:open?)
  end

  def test_invalid_actor_raises
    inner = FakeTransport.new

    assert_raises(ArgumentError) do
      ThriftIllustrated::RecordingTransport.new(inner_transport: inner, actor: "proxy")
    end
  end

  def test_invalid_inner_transport_raises
    assert_raises(ArgumentError) do
      ThriftIllustrated::RecordingTransport.new(inner_transport: Object.new, actor: "client")
    end
  end

  def test_non_string_payload_raises
    inner = FakeTransport.new
    transport = ThriftIllustrated::RecordingTransport.new(inner_transport: inner, actor: "client")

    assert_raises(TypeError) do
      transport.write(123)
    end
  end

  def test_records_are_externally_immutable
    inner = FakeTransport.new
    transport = ThriftIllustrated::RecordingTransport.new(inner_transport: inner, actor: "client")

    transport.write("abc")
    external = transport.records

    assert_raises(FrozenError) { external.first[:actor] = "server" }

    external << { chunk_index: 99 }
    assert_equal 1, transport.records.length
  end
end
