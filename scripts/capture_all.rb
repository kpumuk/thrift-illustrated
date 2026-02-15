#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/setup"
require "digest"
require "fileutils"
require "json"
require "open3"
require "optparse"
require "pathname"
require "securerandom"
require "socket"
require "time"

require_relative "lib/artifact_writer"
require_relative "lib/message_parser"
require_relative "lib/tutorial_capture"
require_relative "lib/validation_pipeline"

class CliError < StandardError; end
class SetupError < StandardError; end

class PipelineError < StandardError
  attr_reader :stage, :code, :severity, :details

  def initialize(message, stage:, code:, severity: nil, details: nil)
    super(message)
    @stage = stage
    @code = code
    @severity = severity
    @details = details
  end
end

class CaptureAll
  COMBOS = {
    "binary-buffered" => { protocol: "binary", transport: "buffered" },
    "binary-framed" => { protocol: "binary", transport: "framed" },
    "compact-buffered" => { protocol: "compact", transport: "buffered" },
    "compact-framed" => { protocol: "compact", transport: "framed" },
    "json-buffered" => { protocol: "json", transport: "buffered" },
    "json-framed" => { protocol: "json", transport: "framed" }
  }.freeze

  STAGES = %w[capture record parse validate write verify].freeze
  DEFAULT_OUTPUT = "data/captures"

  def initialize(argv:, stdout: $stdout, stderr: $stderr, pwd: Dir.pwd)
    @argv = argv.dup
    @stdout = stdout
    @stderr = stderr
    @pwd = Pathname.new(pwd).expand_path
    @options = {
      combo: nil,
      output: DEFAULT_OUTPUT,
      strict: false
    }

    @combo_artifacts = {}
  end

  def run
    parse_options!
    repo_root = resolve_repo_root
    ensure_environment!(repo_root)

    @artifact_writer = ThriftIllustrated::ArtifactWriter.new(base_dir: output_dir_for(repo_root))
    @validator = ThriftIllustrated::ValidationPipeline.new(schema_dir: repo_root.join("data/schemas"))
    @parser = ThriftIllustrated::MessageParser.new

    combos_to_run.each do |combo_id|
      run_combo(combo_id)
    end

    finalize_manifest!(repo_root)
    0
  rescue OptionParser::ParseError, CliError, SetupError => e
    log(
      stage: "setup",
      combo: "all",
      event: "error",
      duration_ms: 0,
      code: "E_SETUP_INVALID",
      severity: "error",
      details: { message: e.message }
    )
    @stderr.puts(e.message)
    2
  rescue PipelineError => e
    log(
      stage: e.stage,
      combo: (e.details && e.details["combo"]) || "all",
      event: "error",
      duration_ms: 0,
      code: e.code,
      severity: e.severity || "error",
      details: e.details || { message: e.message }
    )
    @stderr.puts(e.message)
    e.stage == "write" ? 4 : 3
  rescue StandardError => e
    log(
      stage: "internal",
      combo: "all",
      event: "error",
      duration_ms: 0,
      code: "E_INTERNAL",
      severity: "fatal",
      details: { message: e.message, class: e.class.name }
    )
    @stderr.puts("Unexpected internal failure: #{e.class}: #{e.message}")
    1
  end

  private

  def parse_options!
    parser = OptionParser.new do |opts|
      opts.banner = "Usage: bundle exec ruby scripts/capture_all.rb [--combo <id>] [--output <dir>] [--strict]"

      opts.on("--combo ID", "Run one combo: #{COMBOS.keys.join(", ")}") do |value|
        unless COMBOS.key?(value)
          raise OptionParser::InvalidArgument, "--combo must be one of: #{COMBOS.keys.join(", ")}"
        end

        @options[:combo] = value
      end

      opts.on("--output DIR", "Output directory (must be within repo)") do |value|
        @options[:output] = value
      end

      opts.on("--strict", "Abort on first parse error with severity error or fatal") do
        @options[:strict] = true
      end

      opts.on("-h", "--help", "Show help") do
        @stdout.puts(opts)
        exit(0)
      end
    end

    parser.parse!(@argv)
    raise CliError, "Unexpected positional arguments: #{@argv.join(" ")}" unless @argv.empty?
  end

  def combos_to_run
    @options[:combo] ? [@options[:combo]] : COMBOS.keys
  end

  def run_combo(combo_id)
    combo_meta = COMBOS.fetch(combo_id)
    state = {
      "combo" => combo_id,
      "combo_meta" => combo_meta,
      "strict" => @options[:strict],
      "parse_errors" => []
    }

    STAGES.each do |stage|
      started = monotonic_now
      log(stage: stage, combo: combo_id, event: "start", duration_ms: 0)
      state = run_stage(stage, state)
      log(stage: stage, combo: combo_id, event: "ok", duration_ms: elapsed_ms(started))
    end
  end

  def run_stage(stage, state)
    send("stage_#{stage}", state)
  rescue PipelineError
    raise
  rescue StandardError => e
    raise PipelineError.new(
      "Stage #{stage} failed for #{state.fetch("combo")}: #{e.message}",
      stage: stage,
      code: "E_STAGE_FAILED",
      severity: "error",
      details: {
        "combo" => state.fetch("combo"),
        "stage" => stage,
        "message" => e.message,
        "class" => e.class.name
      }
    )
  end

  def stage_capture(state)
    combo_meta = state.fetch("combo_meta")
    host = "127.0.0.1"
    port = allocate_local_port
    timeout_ms = fetch_timeout_ms

    captured = ThriftIllustrated::TutorialCapture.new(
      protocol: combo_meta.fetch(:protocol),
      transport: combo_meta.fetch(:transport),
      host: host,
      port: port,
      timeout_ms: timeout_ms
    ).run

    state.merge(
      "host" => host,
      "port" => port,
      "captured_at_utc" => Time.now.utc.iso8601,
      "client_records" => captured.fetch(:client_records),
      "server_records" => captured.fetch(:server_records)
    )
  rescue StandardError => e
    raise PipelineError.new(
      "Capture failed for #{state.fetch("combo")}: #{e.message}",
      stage: "capture",
      code: "E_CAPTURE_FAILED",
      severity: "error",
      details: {
        "combo" => state.fetch("combo"),
        "message" => e.message,
        "class" => e.class.name
      }
    )
  end

  def stage_record(state)
    client_records = normalize_records(state.fetch("client_records"))
    server_records = normalize_records(state.fetch("server_records"))

    state.merge(
      "client_records" => client_records,
      "server_records" => server_records,
      "wire_chunk_count" => client_records.length + server_records.length
    )
  end

  def stage_parse(state)
    combo_meta = state.fetch("combo_meta")
    client = @parser.parse_records(
      records: state.fetch("client_records"),
      protocol: combo_meta.fetch(:protocol),
      transport: combo_meta.fetch(:transport),
      actor: "client",
      starting_index: 0
    )
    server = @parser.parse_records(
      records: state.fetch("server_records"),
      protocol: combo_meta.fetch(:protocol),
      transport: combo_meta.fetch(:transport),
      actor: "server",
      starting_index: 0
    )

    merged = interleave_messages(client.fetch(:messages), server.fetch(:messages))
    merged.each_with_index do |message, idx|
      message[:index] = idx
      message.delete(:_field_node_count)
      message.delete(:_max_field_depth)
      message.delete(:_max_string_value_bytes)
    end

    parse_errors = merged.flat_map { |message| Array(message[:parse_errors]) }

    if @options[:strict]
      parse_errors.each_with_index do |error, idx|
        severity = error.fetch(:severity, error["severity"]).to_s
        next if severity == "warning"

        raise PipelineError.new(
          "Strict mode abort for #{state.fetch("combo")}: parse error #{idx} severity=#{severity}",
          stage: "parse",
          code: error.fetch(:code, error["code"]),
          severity: severity,
          details: {
            "combo" => state.fetch("combo"),
            "parse_error_index" => idx,
            "parse_error" => stringify_keys(error)
          }
        )
      end
    end

    stats = {
      "max_field_depth" => [client.fetch(:max_field_depth), server.fetch(:max_field_depth)].max,
      "total_field_nodes" => client.fetch(:total_field_nodes) + server.fetch(:total_field_nodes),
      "max_string_value_bytes" => [client.fetch(:max_string_value_bytes), server.fetch(:max_string_value_bytes)].max,
      "max_highlights_per_message" => merged.map { |message| Array(message[:highlights]).length }.max || 0
    }

    state.merge(
      "messages" => merged,
      "parse_errors" => parse_errors,
      "parse_stats" => stats
    )
  end

  def stage_validate(state)
    fatal_errors = Array(state.fetch("parse_errors")).select do |error|
      severity = error.fetch(:severity, error["severity"]).to_s
      severity == "fatal"
    end

    unless fatal_errors.empty?
      raise PipelineError.new(
        "Validation failed for #{state.fetch("combo")}: fatal parse errors present",
        stage: "validate",
        code: "E_PROTOCOL_DECODE",
        severity: "fatal",
        details: {
          "combo" => state.fetch("combo"),
          "fatal_count" => fatal_errors.length
        }
      )
    end

    dataset = build_dataset(state)
    result = @validator.validate_dataset(dataset)
    unless result.ok?
      first = result.errors.first
      raise PipelineError.new(
        "Validation failed for #{state.fetch("combo")}: #{first[:message]}",
        stage: "validate",
        code: first[:code],
        severity: first[:severity],
        details: {
          "combo" => state.fetch("combo"),
          "errors" => result.errors.map { |entry| stringify_keys(entry) }
        }
      )
    end

    state.merge("dataset" => dataset)
  end

  def stage_write(state)
    combo_id = state.fetch("combo")
    dataset = state.fetch("dataset")
    output_path = @artifact_writer.write_combo(combo_id: combo_id, payload: dataset)
    output_bytes = File.binread(output_path)

    @combo_artifacts[combo_id] = {
      dataset: dataset,
      path: output_path,
      bytes: output_bytes.bytesize,
      sha256: Digest::SHA256.hexdigest(output_bytes),
      message_count: Array(dataset["messages"]).length
    }

    state.merge("combo_path" => output_path.to_s)
  rescue StandardError => e
    raise PipelineError.new(
      "Write failed for #{state.fetch("combo")}: #{e.message}",
      stage: "write",
      code: "E_WRITE_FAILED",
      severity: "error",
      details: {
        "combo" => state.fetch("combo"),
        "message" => e.message,
        "class" => e.class.name
      }
    )
  end

  def stage_verify(state)
    combo_path = Pathname.new(state.fetch("combo_path"))
    raise PipelineError.new("Missing combo artifact for #{state.fetch("combo")}", stage: "verify", code: "E_VERIFY_MISSING", severity: "error", details: { "combo" => state.fetch("combo") }) unless combo_path.file?

    JSON.parse(File.read(combo_path))
    state
  end

  def finalize_manifest!(repo_root)
    started = monotonic_now
    log(stage: "verify", combo: "all", event: "start", duration_ms: 0)

    combos = combos_to_run.map do |combo_id|
      artifact = @combo_artifacts.fetch(combo_id)
      combo_meta = COMBOS.fetch(combo_id)

      {
        "id" => combo_id,
        "protocol" => combo_meta.fetch(:protocol),
        "transport" => combo_meta.fetch(:transport),
        "file" => "#{combo_id}.json",
        "sha256" => artifact.fetch(:sha256),
        "bytes" => artifact.fetch(:bytes),
        "message_count" => artifact.fetch(:message_count)
      }
    end

    manifest = {
      "schema_version" => "1.0.0",
      "release_id" => current_release_id(repo_root),
      "generated_at_utc" => Time.now.utc.iso8601,
      "generated_with" => {
        "ruby_version" => RUBY_VERSION,
        "bundler_version" => Bundler::VERSION,
        "thrift_ref" => resolve_thrift_ref(repo_root),
        "platform" => RUBY_PLATFORM
      },
      "combos" => combos
    }

    @artifact_writer.write_manifest(manifest)

    validation = @validator.validate_all(
      manifest: manifest,
      datasets_by_id: @combo_artifacts.transform_values { |entry| entry.fetch(:dataset) }
    )

    unless validation.ok?
      first = validation.errors.first
      raise PipelineError.new(
        "Cross-file verification failed: #{first[:message]}",
        stage: "verify",
        code: first[:code],
        severity: first[:severity],
        details: {
          "errors" => validation.errors.map { |entry| stringify_keys(entry) }
        }
      )
    end

    log(stage: "verify", combo: "all", event: "ok", duration_ms: elapsed_ms(started))
  end

  def build_dataset(state)
    combo = state.fetch("combo")
    combo_meta = state.fetch("combo_meta")
    stats = state.fetch("parse_stats")

    {
      "schema_version" => "1.0.0",
      "combo" => {
        "id" => combo,
        "protocol" => combo_meta.fetch(:protocol),
        "transport" => combo_meta.fetch(:transport)
      },
      "metadata" => {
        "capture_id" => SecureRandom.uuid,
        "captured_at_utc" => state.fetch("captured_at_utc"),
        "ruby_version" => RUBY_VERSION,
        "bundler_version" => Bundler::VERSION,
        "thrift_ref" => resolve_thrift_ref(resolve_repo_root),
        "platform" => RUBY_PLATFORM,
        "message_count" => Array(state.fetch("messages")).length,
        "wire_chunk_count" => state.fetch("wire_chunk_count"),
        "max_field_depth" => stats.fetch("max_field_depth"),
        "total_field_nodes" => stats.fetch("total_field_nodes"),
        "max_string_value_bytes" => stats.fetch("max_string_value_bytes"),
        "max_highlights_per_message" => stats.fetch("max_highlights_per_message")
      },
      "dataset_errors" => [],
      "messages" => state.fetch("messages").map { |message| stringify_keys(message) }
    }
  end

  def interleave_messages(client_messages, server_messages)
    merged = []
    server_index = 0

    client_messages.each do |message|
      merged << message

      next if message[:message_type] == "oneway"
      next if server_index >= server_messages.length

      merged << server_messages[server_index]
      server_index += 1
    end

    while server_index < server_messages.length
      merged << server_messages[server_index]
      server_index += 1
    end

    merged
  end

  def normalize_records(records)
    Array(records).sort_by do |entry|
      [entry.fetch(:monotonic_ns, 0), entry.fetch(:chunk_index, 0)]
    end.each_with_index.map do |entry, idx|
      entry.merge(chunk_index: idx)
    end
  end

  def resolve_repo_root
    stdout, status = Open3.capture2("git", "rev-parse", "--show-toplevel", chdir: @pwd.to_s)
    return Pathname.new(stdout.strip).expand_path if status.success?

    @pwd
  end

  def output_dir_for(repo_root)
    Pathname.new(@options[:output]).expand_path(repo_root)
  end

  def ensure_environment!(repo_root)
    expected_ruby = expected_ruby_version(repo_root)
    if expected_ruby && RUBY_VERSION != expected_ruby
      raise SetupError, "Ruby version mismatch: expected #{expected_ruby}, got #{RUBY_VERSION}. Run with `mise exec --` or switch toolchain."
    end

    require "thrift"
    ensure_localhost_tcp_available!

    out_dir = output_dir_for(repo_root)
    ensure_within_repo!(path: out_dir, repo_root: repo_root)
    FileUtils.mkdir_p(out_dir)
    ensure_directory_writable!(out_dir)

    %w[manifest.schema.json combo.schema.json benchmark.schema.json runtime-error.schema.json].each do |schema_file|
      schema_path = repo_root.join("data/schemas/#{schema_file}")
      raise SetupError, "Missing schema file #{schema_path}" unless schema_path.file?
    end
  rescue SetupError
    raise
  rescue LoadError => e
    raise SetupError, "Missing runtime dependency: #{e.message}. Run `mise exec -- bundle install`."
  rescue StandardError => e
    raise SetupError, "Environment check failed: #{e.message}"
  end

  def ensure_within_repo!(path:, repo_root:)
    real_path = path.exist? ? path.realpath : path
    real_repo = repo_root.realpath

    return if real_path == real_repo
    return if real_path.to_s.start_with?("#{real_repo}/")

    raise SetupError, "Output directory must stay within repo: #{real_path}"
  end

  def ensure_directory_writable!(dir)
    probe = dir.join(".write_test_#{Process.pid}_#{Time.now.to_i}")
    File.write(probe, "ok\n")
    File.delete(probe)
  rescue StandardError => e
    raise SetupError, "Output directory not writable: #{dir} (#{e.message})"
  end

  def ensure_localhost_tcp_available!
    server = TCPServer.new("127.0.0.1", 0)
    server.close
  rescue StandardError => e
    raise SetupError, "Localhost TCP unavailable: #{e.message}"
  end

  def expected_ruby_version(repo_root)
    mise_file = repo_root.join("mise.toml")
    return nil unless mise_file.file?

    match = mise_file.read.match(/^\s*ruby\s*=\s*"([^"]+)"\s*$/)
    match && match[1]
  end

  def fetch_timeout_ms
    timeout = ENV.fetch("THRIFT_TIMEOUT_MS", "5000").to_i
    timeout.positive? ? timeout : 5000
  end

  def allocate_local_port
    server = TCPServer.new("127.0.0.1", 0)
    port = server.addr[1]
    server.close
    port
  end

  def current_release_id(repo_root)
    stdout, status = Open3.capture2("git", "rev-parse", "HEAD", chdir: repo_root.to_s)
    value = stdout.strip
    return value if status.success? && value.match?(/^[a-f0-9]{40}$/)

    "0" * 40
  end

  def resolve_thrift_ref(repo_root)
    lock_path = repo_root.join("Gemfile.lock")
    return "gem:unknown" unless lock_path.file?

    content = lock_path.read
    match = content.match(%r{remote:\s+https://github.com/apache/thrift.git\s+revision:\s+([a-f0-9]{40})}m)
    return "git:#{match[1]}" if match

    version = content.match(/^\s{4}thrift \(([^)]+)\)$/)&.captures&.first
    return "gem:#{version}" if version

    "gem:unknown"
  end

  def monotonic_now
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def elapsed_ms(started)
    ((monotonic_now - started) * 1000.0).round
  end

  def log(stage:, combo:, event:, duration_ms:, code: nil, severity: nil, details: nil)
    fields = []
    fields << "ts=#{Time.now.utc.iso8601}"
    fields << "stage=#{stage}"
    fields << "combo=#{combo}"
    fields << "event=#{event}"
    fields << "duration_ms=#{duration_ms}"
    fields << "code=#{code}" if code
    fields << "severity=#{severity}" if severity
    fields << "details_json=#{JSON.generate(details)}" if details
    @stdout.puts(fields.join(" "))
  end

  def stringify_keys(value)
    case value
    when Hash
      value.each_with_object({}) { |(k, v), out| out[k.to_s] = stringify_keys(v) }
    when Array
      value.map { |entry| stringify_keys(entry) }
    else
      value
    end
  end
end

exit(CaptureAll.new(argv: ARGV).run)
