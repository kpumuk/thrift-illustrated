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

desc "Run Ruby linter"
task :lint do
  sh "bundle exec standardrb"
end

desc "Run project tests"
task :test do
  sh "bundle exec ruby -Itest -e 'Dir[\"test/*_test.rb\"].sort.each { |f| require_relative f }'"
end

namespace :schema do
  desc "Validate committed captures against schemas and invariants"
  task :captures do
    sh "bundle exec ruby -Itest test/artifact_schema_invariant_test.rb"
  end

  desc "Validate benchmark report JSON against benchmark schema"
  task :benchmark do
    unless File.exist?(BENCHMARK_REPORT_PATH)
      raise "Benchmark report missing at #{BENCHMARK_REPORT_PATH}. Run `mise run schema-benchmark` first."
    end

    validate_schema!(schema_path: BENCHMARK_SCHEMA_PATH, data_path: BENCHMARK_REPORT_PATH)
  end
end

desc "Run Ruby schema validation checks"
task schema: ["schema:captures"]

desc "Run full Ruby quality checks"
task check: %i[lint test schema]

def validate_schema!(schema_path:, data_path:)
  schema = JSON.parse(File.read(schema_path))
  data = JSON.parse(File.read(data_path))
  errors = JSONSchemer.schema(schema).validate(data).to_a
  return if errors.empty?

  details = errors.map { |error| "#{error['data_pointer']}: #{error['type']}" }
  raise "Schema validation failed for #{data_path}:\n#{details.join("\n")}"
end
