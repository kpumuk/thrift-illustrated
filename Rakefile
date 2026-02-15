# frozen_string_literal: true

require "json"
require "json_schemer"
require "rake"

BENCHMARK_REPORT_PATH = "tmp/benchmarks/latest.json"
BENCHMARK_SCHEMA_PATH = "site/data/schemas/benchmark.schema.json"

desc "Capture tutorial traffic and generate artifacts"
task :capture do
  sh "bundle exec ruby scripts/capture_all.rb"
end

desc "Run project tests"
task :test do
  sh "bundle exec ruby -Itest -e 'Dir[\"test/*_test.rb\"].sort.each { |f| require_relative f }'"
end

desc "Build static output (source-served; placeholder)"
task :build do
  puts "build: bootstrap placeholder (no build pipeline yet)"
end

desc "Run full quality checks"
task check: %i[test] do
  sh "bun run scripts/benchmark_ui.mjs --output #{BENCHMARK_REPORT_PATH}"
  validate_schema!(schema_path: BENCHMARK_SCHEMA_PATH, data_path: BENCHMARK_REPORT_PATH)
end

def validate_schema!(schema_path:, data_path:)
  schema = JSON.parse(File.read(schema_path))
  data = JSON.parse(File.read(data_path))
  errors = JSONSchemer.schema(schema).validate(data).to_a
  return if errors.empty?

  details = errors.map { |error| "#{error['data_pointer']}: #{error['type']}" }
  raise "Schema validation failed for #{data_path}:\n#{details.join("\n")}"
end
