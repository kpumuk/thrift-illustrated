# frozen_string_literal: true

require "minitest/autorun"
require_relative "../scripts/lib/validation_pipeline"

class ValidationPipelineTest < Minitest::Test
  def test_validate_all_success_for_valid_documents
    pipeline = ThriftIllustrated::ValidationPipeline.new(schema_dir: "data/schemas")

    manifest = valid_manifest(message_count: 7)
    dataset = valid_dataset(methods: %w[ping add add calculate getStruct calculate zip])

    result = pipeline.validate_all(manifest: manifest, datasets_by_id: { "binary-buffered" => dataset })

    assert result.ok?, result.errors.inspect
  end

  def test_message_count_mismatch_detected
    pipeline = ThriftIllustrated::ValidationPipeline.new(schema_dir: "data/schemas")

    manifest = valid_manifest(message_count: 7)
    dataset = valid_dataset(methods: %w[ping add add calculate getStruct calculate zip])
    dataset["metadata"]["message_count"] = 6

    result = pipeline.validate_all(manifest: manifest, datasets_by_id: { "binary-buffered" => dataset })

    codes = result.errors.map { |e| e[:code] }
    assert_includes codes, "E_MESSAGE_COUNT_MISMATCH"
  end

  def test_tutorial_flow_mismatch_detected
    pipeline = ThriftIllustrated::ValidationPipeline.new(schema_dir: "data/schemas")

    manifest = valid_manifest(message_count: 7)
    dataset = valid_dataset(methods: %w[ping add add calculate getStruct add zip])

    result = pipeline.validate_all(manifest: manifest, datasets_by_id: { "binary-buffered" => dataset })

    codes = result.errors.map { |e| e[:code] }
    assert_includes codes, "E_TUTORIAL_FLOW_MISMATCH"
  end

  def test_field_ordering_mismatch_detected
    pipeline = ThriftIllustrated::ValidationPipeline.new(schema_dir: "data/schemas")

    manifest = valid_manifest(message_count: 7)
    dataset = valid_dataset(methods: %w[ping add add calculate getStruct calculate zip])
    dataset["messages"][0]["payload"]["fields"] = [
      field_node(id: 2, start_pos: 2, end_pos: 4),
      field_node(id: 1, start_pos: 1, end_pos: 2)
    ]
    dataset["messages"][0]["payload"]["span"] = [1, 4]
    dataset["messages"][0]["transport"]["payload_span"] = [0, 4]
    dataset["messages"][0]["raw_size"] = 4
    dataset["messages"][0]["raw_hex"] = "00 01 02 03"

    result = pipeline.validate_all(manifest: manifest, datasets_by_id: { "binary-buffered" => dataset })

    messages = result.errors.map { |e| e[:message] }
    assert(messages.any? { |msg| msg.include?("Field ordering is not canonical") }, result.errors.inspect)
  end

  private

  def valid_manifest(message_count:)
    {
      "schema_version" => "1.0.0",
      "release_id" => "a" * 40,
      "generated_at_utc" => "2026-02-15T00:00:00Z",
      "generated_with" => {
        "ruby_version" => "4.0.1",
        "bundler_version" => "4.0.6",
        "thrift_ref" => "git:" + ("b" * 40),
        "platform" => "arm64-darwin"
      },
      "combos" => [
        {
          "id" => "binary-buffered",
          "protocol" => "binary",
          "transport" => "buffered",
          "file" => "binary-buffered.json",
          "sha256" => "c" * 64,
          "bytes" => 1024,
          "message_count" => message_count
        }
      ]
    }
  end

  def valid_dataset(methods:)
    {
      "schema_version" => "1.0.0",
      "combo" => {
        "id" => "binary-buffered",
        "protocol" => "binary",
        "transport" => "buffered"
      },
      "metadata" => {
        "capture_id" => "capture-1",
        "captured_at_utc" => "2026-02-15T00:00:00Z",
        "ruby_version" => "4.0.1",
        "bundler_version" => "4.0.6",
        "thrift_ref" => "git:" + ("b" * 40),
        "platform" => "arm64-darwin",
        "message_count" => methods.length,
        "wire_chunk_count" => methods.length,
        "max_field_depth" => 0,
        "total_field_nodes" => 0,
        "max_string_value_bytes" => 0,
        "max_highlights_per_message" => 0
      },
      "dataset_errors" => [],
      "messages" => methods.each_with_index.map do |method, idx|
        {
          "index" => idx,
          "actor" => "client",
          "direction" => "client->server",
          "method" => method,
          "message_type" => (method == "zip" ? "oneway" : "call"),
          "seqid" => idx,
          "raw_hex" => "00 01",
          "raw_size" => 2,
          "transport" => {
            "type" => "buffered",
            "frame_length" => nil,
            "frame_header_span" => nil,
            "payload_span" => [0, 2]
          },
          "envelope" => {
            "name" => method,
            "type" => (method == "zip" ? "oneway" : "call"),
            "seqid" => idx,
            "span" => [0, 1]
          },
          "payload" => {
            "struct" => {
              "name" => "args",
              "ttype" => "struct",
              "span" => [1, 2]
            },
            "span" => [1, 2],
            "fields" => []
          },
          "highlights" => [],
          "parse_errors" => []
        }
      end
    }
  end

  def field_node(id:, start_pos:, end_pos:)
    {
      "id" => id,
      "name" => "field_#{id}",
      "ttype" => "i32",
      "span" => [start_pos, end_pos],
      "value" => id,
      "children" => []
    }
  end
end
