#!/usr/bin/env ruby
# frozen_string_literal: true

require "digest"
require "json"
require "net/http"
require "open3"
require "optparse"
require "time"
require "uri"

class HealthCheck
  DEFAULT_TIMEOUT_MS = 5000
  DEFAULT_RETRIES = 2
  DEFAULT_COMBO = "binary-buffered"
  DEFAULT_MSG = 0
  DEFAULT_ALERT_DESTINATION = "#deploy-alerts"

  def initialize(argv:, stdout: $stdout, stderr: $stderr)
    @argv = argv.dup
    @stdout = stdout
    @stderr = stderr
    @options = {
      base_url: nil,
      combo: DEFAULT_COMBO,
      msg: DEFAULT_MSG,
      timeout_ms: DEFAULT_TIMEOUT_MS,
      retries: DEFAULT_RETRIES,
      enforce_headers: true,
      alert_webhook: nil,
      alert_file: nil,
      alert_destination: DEFAULT_ALERT_DESTINATION
    }
    @release_id = nil
  end

  def run
    parse_options!
    base_uri = normalize_base_uri(@options.fetch(:base_url))
    repo_root = File.expand_path("..", __dir__)

    manifest_response = nil
    manifest_body = nil
    manifest_json = nil
    combo_response = nil
    combo_body = nil
    combo_entry = nil

    success = true

    success &&= run_check("manifest_fetch") do
      manifest_uri = URI.join(base_uri.to_s, "data/captures/manifest.json")
      manifest_response, manifest_body = fetch_resource(manifest_uri)
      assert_json_response!(manifest_response, resource_name: "manifest")
      assert_required_headers!(manifest_response, resource_name: "manifest") if @options.fetch(:enforce_headers)
      manifest_json = parse_json(manifest_body, resource_name: "manifest")
      @release_id = manifest_json["release_id"]
      { bytes: manifest_body.bytesize, release_id: @release_id }
    end

    success &&= run_check("combo_fetch") do
      combo_entry = Array(manifest_json["combos"]).find { |entry| entry["id"] == @options.fetch(:combo) }
      raise "Combo #{@options.fetch(:combo)} not found in manifest." unless combo_entry

      combo_uri = URI.join(base_uri.to_s, "data/captures/#{combo_entry.fetch("file")}")
      combo_response, combo_body = fetch_resource(combo_uri)
      assert_json_response!(combo_response, resource_name: combo_entry.fetch("id"))
      assert_required_headers!(combo_response, resource_name: combo_entry.fetch("id")) if @options.fetch(:enforce_headers)
      parse_json(combo_body, resource_name: combo_entry.fetch("id"))
      { bytes: combo_body.bytesize, combo: combo_entry.fetch("id") }
    end

    success &&= run_check("dataset_bytes") do
      expected_bytes = Integer(combo_entry.fetch("bytes"))
      observed_bytes = combo_body.bytesize
      raise "Dataset byte length mismatch: expected #{expected_bytes}, got #{observed_bytes}" unless observed_bytes == expected_bytes

      { expected: expected_bytes, observed: observed_bytes, combo: combo_entry.fetch("id") }
    end

    success &&= run_check("dataset_sha256") do
      expected_sha = combo_entry.fetch("sha256").to_s.downcase
      observed_sha = Digest::SHA256.hexdigest(combo_body)
      raise "Dataset SHA mismatch: expected #{expected_sha}, got #{observed_sha}" unless observed_sha == expected_sha

      { expected: expected_sha, observed: observed_sha, combo: combo_entry.fetch("id") }
    end

    success &&= run_check("browser_render") do
      browser_url = "#{URI.join(base_uri.to_s, "/")}#combo=#{URI.encode_www_form_component(@options.fetch(:combo))}&msg=#{@options.fetch(:msg)}"
      stdout, stderr, status = Open3.capture3(
        "bun",
        "run",
        "scripts/health_check_browser.mjs",
        "--url", browser_url,
        "--timeout-ms", @options.fetch(:timeout_ms).to_s,
        chdir: repo_root
      )
      raise "Browser render check failed: #{stderr.empty? ? stdout : stderr}" unless status.success?

      JSON.parse(stdout.lines.last)
    end

    success ? 0 : 1
  rescue OptionParser::ParseError => error
    @stderr.puts(error.message)
    2
  rescue StandardError => error
    emit_alert(check: "health_check_setup", message: error.message, threshold: nil, observed: nil)
    @stderr.puts(error.message)
    1
  end

  private

  def parse_options!
    parser = OptionParser.new do |opts|
      opts.banner = "Usage: bundle exec ruby scripts/health_check.rb --base-url <url> [options]"
      opts.on("--base-url URL", "Base URL that serves the site root and /data/captures") { |value| @options[:base_url] = value }
      opts.on("--combo ID", "Combo id for dataset/browser checks (default: #{DEFAULT_COMBO})") { |value| @options[:combo] = value }
      opts.on("--msg N", Integer, "Message index for browser check (default: #{DEFAULT_MSG})") { |value| @options[:msg] = value }
      opts.on("--timeout-ms N", Integer, "HTTP/browser timeout in milliseconds (default: #{DEFAULT_TIMEOUT_MS})") { |value| @options[:timeout_ms] = value }
      opts.on("--retries N", Integer, "Retries per check after first failure (default: #{DEFAULT_RETRIES})") { |value| @options[:retries] = value }
      opts.on("--skip-header-checks", "Skip cache/security header checks (use only for local debugging)") { @options[:enforce_headers] = false }
      opts.on("--alert-webhook URL", "Webhook endpoint for release-gate alerts") { |value| @options[:alert_webhook] = value }
      opts.on("--alert-file PATH", "Append alert payload JSON lines to file") { |value| @options[:alert_file] = value }
      opts.on("--alert-destination NAME", "Alert destination label (default: #{DEFAULT_ALERT_DESTINATION})") { |value| @options[:alert_destination] = value }
      opts.on("-h", "--help", "Show this help") do
        @stdout.puts(opts)
        exit(0)
      end
    end

    parser.parse!(@argv)
    raise OptionParser::MissingArgument, "--base-url is required" unless @options[:base_url]
    raise OptionParser::InvalidArgument, "--msg must be >= 0" if @options.fetch(:msg).negative?
    raise OptionParser::InvalidArgument, "--timeout-ms must be > 0" unless @options.fetch(:timeout_ms).positive?
    raise OptionParser::InvalidArgument, "--retries must be >= 0" if @options.fetch(:retries).negative?
  end

  def normalize_base_uri(raw)
    uri = URI.parse(raw)
    raise OptionParser::InvalidArgument, "--base-url must be absolute (http/https)" unless uri.is_a?(URI::HTTP) || uri.is_a?(URI::HTTPS)

    uri.path = "#{uri.path}/" unless uri.path.end_with?("/")
    uri
  rescue URI::InvalidURIError => error
    raise OptionParser::InvalidArgument, "--base-url is invalid: #{error.message}"
  end

  def run_check(check_name)
    retries = @options.fetch(:retries)
    attempt = 0

    begin
      attempt += 1
      details = yield
      log(check: check_name, status: "pass", details: details)
      true
    rescue StandardError => error
      details = { message: error.message, attempt: attempt }
      if attempt <= retries
        log(check: check_name, status: "fail", details: details.merge(retrying: true))
        retry
      end

      log(check: check_name, status: "fail", details: details)
      emit_alert(check: check_name, message: error.message, threshold: nil, observed: nil)
      false
    end
  end

  def fetch_resource(uri)
    timeout_seconds = @options.fetch(:timeout_ms) / 1000.0
    response = nil
    body = nil

    Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == "https", open_timeout: timeout_seconds, read_timeout: timeout_seconds) do |http|
      request = Net::HTTP::Get.new(uri.request_uri)
      response = http.request(request)
      body = response.body.to_s.b
    end

    unless response.code.to_i == 200
      raise "HTTP #{response.code} for #{uri}"
    end

    [response, body]
  rescue Net::OpenTimeout, Net::ReadTimeout => error
    raise "Request timed out for #{uri}: #{error.class}"
  rescue SocketError => error
    raise "Network error for #{uri}: #{error.message}"
  end

  def parse_json(bytes, resource_name:)
    JSON.parse(bytes)
  rescue JSON::ParserError => error
    raise "Invalid JSON for #{resource_name}: #{error.message}"
  end

  def assert_json_response!(response, resource_name:)
    content_type = response["content-type"].to_s
    return if content_type.downcase.start_with?("application/json")

    raise "Unexpected content-type for #{resource_name}: #{content_type.inspect}"
  end

  def assert_required_headers!(response, resource_name:)
    cache_control = response["cache-control"].to_s
    x_content_type = response["x-content-type-options"].to_s.downcase
    referrer_policy = response["referrer-policy"].to_s.downcase

    raise "Missing required cache-control=no-cache for #{resource_name}" unless cache_control.downcase.include?("no-cache")
    raise "Missing required x-content-type-options=nosniff for #{resource_name}" unless x_content_type == "nosniff"
    raise "Missing required referrer-policy=no-referrer for #{resource_name}" unless referrer_policy == "no-referrer"
  end

  def log(check:, status:, details:)
    line = "ts=#{Time.now.utc.iso8601} check=#{check} status=#{status}"
    line += " details_json=#{JSON.generate(details)}" if details
    @stdout.puts(line)
  end

  def emit_alert(check:, message:, threshold:, observed:)
    payload = {
      release_id: @release_id,
      check_name: check,
      threshold: threshold,
      observed: observed,
      destination: @options.fetch(:alert_destination),
      message: message,
      ts: Time.now.utc.iso8601
    }

    @stderr.puts("ALERT #{JSON.generate(payload)}")
    append_alert_file(payload) if @options[:alert_file]
    post_alert_webhook(payload) if @options[:alert_webhook]
  end

  def append_alert_file(payload)
    path = File.expand_path(@options.fetch(:alert_file), Dir.pwd)
    File.open(path, "a") { |file| file.puts(JSON.generate(payload)) }
  rescue StandardError => error
    @stderr.puts("Failed to write alert file: #{error.message}")
  end

  def post_alert_webhook(payload)
    uri = URI.parse(@options.fetch(:alert_webhook))
    request = Net::HTTP::Post.new(uri.request_uri)
    request["content-type"] = "application/json"
    request.body = JSON.generate(payload)
    Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == "https") { |http| http.request(request) }
  rescue StandardError => error
    @stderr.puts("Failed to post alert webhook: #{error.message}")
  end
end

exit(HealthCheck.new(argv: ARGV).run)
