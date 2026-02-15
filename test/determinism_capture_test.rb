# frozen_string_literal: true

require "json"
require "minitest/autorun"
require "open3"
require "tmpdir"

class DeterminismCaptureTest < Minitest::Test
  def test_capture_outputs_are_stable_after_normalization
    Dir.mktmpdir("capture-determinism", File.join(repo_root, "tmp")) do |tmpdir|
      run_a = File.join(tmpdir, "run-a")
      run_b = File.join(tmpdir, "run-b")

      run_capture!(run_a)
      run_capture!(run_b)

      normalized_a = normalized_snapshot(run_a)
      normalized_b = normalized_snapshot(run_b)

      assert_equal normalized_a, normalized_b
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

  def normalized_snapshot(output_dir)
    manifest_path = File.join(output_dir, "manifest.json")
    manifest = JSON.parse(File.read(manifest_path))
    manifest["generated_at_utc"] = "__normalized_timestamp__"
    manifest.fetch("combos").each { |entry| entry["sha256"] = "__normalized_sha256__" }

    datasets = manifest.fetch("combos").to_h do |entry|
      dataset = JSON.parse(File.read(File.join(output_dir, entry.fetch("file"))))
      dataset["metadata"]["capture_id"] = "__normalized_capture_id__"
      dataset["metadata"]["captured_at_utc"] = "__normalized_timestamp__"
      [entry.fetch("id"), dataset]
    end

    {
      "manifest" => manifest,
      "datasets" => datasets
    }
  end

  def repo_root
    @repo_root ||= File.expand_path("..", __dir__)
  end
end
