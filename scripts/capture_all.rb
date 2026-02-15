#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/setup"
require "fileutils"
require "json"
require "optparse"
require "open3"
require "pathname"
require "socket"
require "time"

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
    "compact-framed" => { protocol: "compact", transport: "framed" }
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
  end

  def run
    parse_options!
    repo_root = resolve_repo_root
    ensure_environment!(repo_root)

    combos_to_run.each do |combo_id|
      run_combo(combo_id, repo_root)
    end

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

  def run_combo(combo_id, repo_root)
    combo_meta = COMBOS.fetch(combo_id)
    state = {
      "combo" => combo_id,
      "combo_meta" => combo_meta,
      "output_dir" => output_dir_for(repo_root).to_s,
      "strict" => @options[:strict],
      "parse_errors" => []
    }

    STAGES.each do |stage|
      started = monotonic_now
      log(stage: stage, combo: combo_id, event: "start", duration_ms: 0)
      state = run_stage(stage, state)
      log(stage: stage, combo: combo_id, event: "ok", duration_ms: elapsed_ms(started))
    end

    nil
  end

  def run_stage(stage, state)
    method_name = "stage_#{stage}"
    send(method_name, state)
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
    # Capture logic will be implemented as dedicated transport wrappers and parser
    # are added in subsequent tasks.
    state.merge(
      "raw_chunks" => [],
      "captured_at_utc" => Time.now.utc.iso8601
    )
  end

  def stage_record(state)
    state
  end

  def stage_parse(state)
    parse_errors = Array(state["parse_errors"])
    return state if parse_errors.empty?

    if @options[:strict]
      parse_errors.each_with_index do |entry, index|
        severity = parse_error_severity(entry)
        next if severity == "warning"

        raise PipelineError.new(
          "Strict mode abort for #{state.fetch("combo")}: parse error #{index} severity=#{severity}",
          stage: "parse",
          code: parse_error_code(entry),
          severity: severity,
          details: {
            "combo" => state.fetch("combo"),
            "parse_error_index" => index,
            "parse_error" => entry
          }
        )
      end
    end

    state
  end

  def stage_validate(state)
    parse_errors = Array(state["parse_errors"])
    fatal_errors = parse_errors.select { |entry| parse_error_severity(entry) == "fatal" }
    return state if fatal_errors.empty?

    raise PipelineError.new(
      "Validation failed for #{state.fetch("combo")}: fatal parse errors present",
      stage: "validate",
      code: "E_PARSE_FATAL",
      severity: "fatal",
      details: {
        "combo" => state.fetch("combo"),
        "fatal_count" => fatal_errors.length
      }
    )
  end

  def stage_write(state)
    output_dir = Pathname.new(state.fetch("output_dir"))
    FileUtils.mkdir_p(output_dir)

    run_metadata = {
      "combo" => state.fetch("combo"),
      "protocol" => state.fetch("combo_meta").fetch(:protocol),
      "transport" => state.fetch("combo_meta").fetch(:transport),
      "strict" => state.fetch("strict"),
      "captured_at_utc" => state.fetch("captured_at_utc"),
      "status" => "placeholder"
    }

    out_file = output_dir.join("#{state.fetch("combo")}.run.json")
    File.write(out_file, "#{JSON.pretty_generate(run_metadata)}\n")

    state.merge("run_file" => out_file.to_s)
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
    run_file = state.fetch("run_file")
    raise PipelineError.new("Missing run artifact for #{state.fetch("combo")}", stage: "verify", code: "E_VERIFY_MISSING", severity: "error", details: { "combo" => state.fetch("combo") }) unless File.file?(run_file)

    state
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
  rescue SetupError
    raise
  rescue LoadError => e
    raise SetupError, "Missing runtime dependency: #{e.message}. Run `mise exec -- bundle install`."
  rescue StandardError => e
    raise SetupError, "Environment check failed: #{e.message}"
  end

  def resolve_repo_root
    stdout, status = Open3.capture2("git", "rev-parse", "--show-toplevel", chdir: @pwd.to_s)
    return Pathname.new(stdout.strip).expand_path if status.success?

    @pwd
  end

  def output_dir_for(repo_root)
    Pathname.new(@options[:output]).expand_path(repo_root)
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

  def parse_error_severity(entry)
    value = if entry.respond_to?(:[])
      entry["severity"] || entry[:severity]
    end
    value.to_s.empty? ? "error" : value.to_s
  end

  def parse_error_code(entry)
    value = if entry.respond_to?(:[])
      entry["code"] || entry[:code]
    end
    value.to_s.empty? ? "E_PARSE_ERROR" : value.to_s
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
end

exit(CaptureAll.new(argv: ARGV).run)
