# frozen_string_literal: true

require "fileutils"
require "json"
require "securerandom"

module ThriftIllustrated
  class ArtifactWriter
    def initialize(base_dir:)
      @base_dir = Pathname.new(base_dir).expand_path
    end

    attr_reader :base_dir

    def write_manifest(payload)
      write_json(relative_path: "manifest.json", payload: payload)
    end

    def write_combo(combo_id:, payload:)
      write_json(relative_path: "#{combo_id}.json", payload: payload)
    end

    def write_json(relative_path:, payload:)
      destination = resolve_destination(relative_path)
      FileUtils.mkdir_p(destination.dirname)

      data = canonical_json(payload)
      atomic_write(destination, data)

      destination
    end

    def canonical_json(payload)
      normalized = canonicalize(payload)
      formatted = JSON.pretty_generate(
        normalized,
        indent: "  ",
        space: " ",
        object_nl: "\n",
        array_nl: "\n"
      )

      ensure_utf8!(formatted)
      formatted.end_with?("\n") ? formatted : "#{formatted}\n"
    end

    private

    def resolve_destination(relative_path)
      relative = Pathname.new(relative_path.to_s)
      raise ArgumentError, "relative_path must not be absolute" if relative.absolute?

      candidate = @base_dir.join(relative).cleanpath
      within_base = candidate.to_s == @base_dir.to_s || candidate.to_s.start_with?("#{@base_dir}/")
      raise ArgumentError, "relative_path escapes base_dir: #{relative_path}" unless within_base

      candidate
    end

    def canonicalize(value)
      case value
      when Hash
        value.keys.map(&:to_s).sort.each_with_object({}) do |key, out|
          original_key = value.key?(key) ? key : value.keys.find { |k| k.to_s == key }
          out[key] = canonicalize(value.fetch(original_key))
        end
      when Array
        value.map { |item| canonicalize(item) }
      when String
        ensure_utf8!(value)
      else
        value
      end
    end

    def ensure_utf8!(string)
      return string if string.encoding == Encoding::UTF_8 && string.valid_encoding?

      utf8 = string.encode("UTF-8")
      raise EncodingError, "String is not valid UTF-8" unless utf8.valid_encoding?

      utf8
    rescue Encoding::UndefinedConversionError, Encoding::InvalidByteSequenceError
      raise EncodingError, "String is not valid UTF-8"
    end

    def atomic_write(destination, data)
      tmp = destination.dirname.join(".#{destination.basename}.tmp-#{Process.pid}-#{SecureRandom.hex(6)}")
      File.write(tmp, data)
      File.rename(tmp, destination)
    ensure
      File.delete(tmp) if tmp && File.exist?(tmp)
    end
  end
end
