# frozen_string_literal: true

require "json"
require "minitest/autorun"
require_relative "../scripts/lib/validation_pipeline"

class ArtifactSchemaInvariantTest < Minitest::Test
  def test_committed_capture_artifacts_validate_against_schema_and_invariants
    manifest_path = File.join(repo_root, "data", "captures", "manifest.json")
    manifest = JSON.parse(File.read(manifest_path))
    datasets = manifest.fetch("combos").to_h do |entry|
      dataset_path = File.join(repo_root, "data", "captures", entry.fetch("file"))
      [entry.fetch("id"), JSON.parse(File.read(dataset_path))]
    end

    pipeline = ThriftIllustrated::ValidationPipeline.new(schema_dir: File.join(repo_root, "data", "schemas"))
    result = pipeline.validate_all(manifest: manifest, datasets_by_id: datasets)

    assert(result.ok?, "Artifact validation errors:\n#{result.errors.map { |error| error[:message] }.join("\n")}")
  end

  private

  def repo_root
    @repo_root ||= File.expand_path("..", __dir__)
  end
end
