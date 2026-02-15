# frozen_string_literal: true

require "rake"

desc "Capture tutorial traffic and generate artifacts"
task :capture do
  sh "bundle exec ruby scripts/capture_all.rb"
end

desc "Run project tests (placeholder)"
task :test do
  puts "test: bootstrap placeholder (test suite not implemented yet)"
end

desc "Build static output (source-served; placeholder)"
task :build do
  puts "build: bootstrap placeholder (no build pipeline yet)"
end

desc "Run full quality checks (placeholder)"
task check: %i[test] do
  puts "check: bootstrap placeholder (schema/determinism/perf checks pending)"
end
