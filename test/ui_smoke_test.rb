# frozen_string_literal: true

require "minitest/autorun"
require "open3"

class UiSmokeTest < Minitest::Test
  def test_ui_smoke_and_error_paths
    stdout, stderr, status = Open3.capture3("bun", "run", "scripts/ui_smoke_test.mjs", chdir: repo_root)
    assert(status.success?, "UI smoke checks failed\nSTDOUT:\n#{stdout}\nSTDERR:\n#{stderr}")
  end

  private

  def repo_root
    @repo_root ||= File.expand_path("..", __dir__)
  end
end
