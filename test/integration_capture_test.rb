# frozen_string_literal: true

require "json"
require "minitest/autorun"
require "open3"
require "tmpdir"

class IntegrationCaptureTest < Minitest::Test
  EXPECTED_COMBOS = %w[
    binary-buffered
    binary-framed
    compact-buffered
    compact-framed
    json-buffered
    json-framed
  ].freeze

  EXPECTED_CLIENT_METHOD_FLOW = [
    "ping:call",
    "add:call",
    "add:call",
    "calculate:call",
    "getStruct:call",
    "calculate:call",
    "zip:oneway"
  ].freeze
  EXPECTED_CLIENT_SEQIDS = [1, 2, 3, 4, 5, 6, 7].freeze
  EXPECTED_SERVER_SEQIDS = [1, 2, 3, 4, 5, 6].freeze
  EXPECTED_ZIP_FIELD_NAMES = %w[
    bool_value
    byte_value
    i16_value
    i32_value
    i64_value
    double_value
    string_value
    binary_value
    list_value
    set_value
    map_value
    struct_value
    struct_list_value
    struct_map_value
    typedef_value
    enum_value
    optional_text
  ].freeze

  def test_capture_all_generates_expected_combo_artifacts_and_tutorial_flow
    Dir.mktmpdir("capture-integration", File.join(repo_root, "tmp")) do |output_dir|
      run_capture!(output_dir)

      manifest = read_json(File.join(output_dir, "manifest.json"))
      combo_ids = manifest.fetch("combos").map { |entry| entry.fetch("id") }
      assert_equal EXPECTED_COMBOS, combo_ids

      manifest.fetch("combos").each do |entry|
        dataset_path = File.join(output_dir, entry.fetch("file"))
        assert(File.file?(dataset_path), "Missing dataset file #{dataset_path}")

        dataset = read_json(dataset_path)
        messages = dataset.fetch("messages")

        assert_equal 13, messages.length
        assert_equal messages.length, dataset.fetch("metadata").fetch("message_count")

        client_method_flow = messages
          .select { |message| message.fetch("direction") == "client->server" }
          .map { |message| "#{message.fetch('method')}:#{message.fetch('message_type')}" }
        assert_equal EXPECTED_CLIENT_METHOD_FLOW, client_method_flow
        client_seqids = messages
          .select { |message| message.fetch("direction") == "client->server" }
          .map { |message| message.fetch("seqid") }
        assert_equal EXPECTED_CLIENT_SEQIDS, client_seqids

        server_seqids = messages
          .select { |message| message.fetch("direction") == "server->client" }
          .map { |message| message.fetch("seqid") }
        assert_equal EXPECTED_SERVER_SEQIDS, server_seqids

        zip_messages = messages.select { |message| message.fetch("method") == "zip" }
        assert_equal 1, zip_messages.length
        assert_equal "oneway", zip_messages.first.fetch("message_type")
        assert_equal "client->server", zip_messages.first.fetch("direction")
        zip_payload_struct = zip_messages.first
          .fetch("payload")
          .fetch("fields")
          .fetch(0)
          .fetch("children")
          .fetch(0)
        assert_equal((1..17).to_a, zip_payload_struct.fetch("children").map { |field| field.fetch("id") })
        assert_equal EXPECTED_ZIP_FIELD_NAMES, zip_payload_struct.fetch("children").map { |field| field.fetch("name") }
        zip_types = zip_payload_struct.fetch("children").map { |field| field.fetch("ttype") }
        %w[bool byte i16 i32 i64 double string list set map struct].each do |ttype|
          assert_includes zip_types, ttype
        end
      end
    end
  end

  private

  def run_capture!(output_dir)
    stdout, stderr, status = Open3.capture3(
      "bundle", "exec", "ruby", "scripts/capture_all.rb", "--output", output_dir,
      chdir: repo_root
    )
    assert(status.success?, "capture_all failed\nSTDOUT:\n#{stdout}\nSTDERR:\n#{stderr}")
  end

  def read_json(path)
    JSON.parse(File.read(path))
  end

  def repo_root
    @repo_root ||= File.expand_path("..", __dir__)
  end
end
