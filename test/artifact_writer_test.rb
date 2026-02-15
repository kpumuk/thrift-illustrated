# frozen_string_literal: true

require "minitest/autorun"
require "tmpdir"
require_relative "../scripts/lib/artifact_writer"

class ArtifactWriterTest < Minitest::Test
  def with_tmpdir
    Dir.mktmpdir("artifact-writer") do |dir|
      yield(dir)
    end
  end

  def test_canonical_json_sorting_and_trailing_newline
    with_tmpdir do |dir|
      writer = ThriftIllustrated::ArtifactWriter.new(base_dir: dir)
      payload = {
        "z" => 1,
        :a => { "d" => 4, "b" => 2 },
        "list" => [{ "k" => 3, "a" => 1 }]
      }

      out_path = writer.write_json(relative_path: "nested/out.json", payload: payload)
      body = File.read(out_path)

      assert body.end_with?("\n")
      assert_match(/\A\{\n/, body)
      assert_operator body.index('"a"'), :<, body.index('"list"')
      assert_operator body.index('"list"'), :<, body.index('"z"')
      assert_operator body.index('"b"'), :<, body.index('"d"')
    end
  end

  def test_repeated_writes_are_byte_stable
    with_tmpdir do |dir|
      writer = ThriftIllustrated::ArtifactWriter.new(base_dir: dir)
      payload = { "b" => 2, "a" => 1 }

      out_path = writer.write_manifest(payload)
      first = File.binread(out_path)

      writer.write_manifest(payload)
      second = File.binread(out_path)

      assert_equal first, second
    end
  end

  def test_rejects_path_escape
    with_tmpdir do |dir|
      writer = ThriftIllustrated::ArtifactWriter.new(base_dir: dir)

      assert_raises(ArgumentError) do
        writer.write_json(relative_path: "../escape.json", payload: { "x" => 1 })
      end
    end
  end

  def test_rejects_invalid_utf8_strings
    with_tmpdir do |dir|
      writer = ThriftIllustrated::ArtifactWriter.new(base_dir: dir)
      invalid = "\xC3\x28".b.force_encoding("UTF-8")

      assert_raises(EncodingError) do
        writer.write_json(relative_path: "bad.json", payload: { "bad" => invalid })
      end
    end
  end
end
