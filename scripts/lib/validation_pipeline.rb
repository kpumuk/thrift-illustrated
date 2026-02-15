# frozen_string_literal: true

require "json_schemer"
require "pathname"

module ThriftIllustrated
  class ValidationPipeline
    EXPECTED_CALL_SEQUENCE = %w[ping add add calculate getStruct calculate zip].freeze

    Result = Struct.new(:errors, keyword_init: true) do
      def ok?
        errors.empty?
      end
    end

    def initialize(schema_dir: "data/schemas")
      @schema_dir = Pathname.new(schema_dir).expand_path
      @manifest_schemer = load_schema("manifest.schema.json")
      @combo_schemer = load_schema("combo.schema.json")
    end

    def validate_manifest(manifest)
      errors = schema_errors(@manifest_schemer, manifest, default_code: "E_PROTOCOL_DECODE")
      Result.new(errors: errors)
    end

    def validate_dataset(dataset)
      errors = []
      errors.concat(schema_errors(@combo_schemer, dataset, default_code: "E_PROTOCOL_DECODE"))
      errors.concat(validate_message_count(dataset))
      errors.concat(validate_message_indices(dataset))
      errors.concat(validate_span_invariants(dataset))
      errors.concat(validate_field_ordering(dataset))
      errors.concat(validate_tutorial_flow(dataset))

      Result.new(errors: errors)
    end

    def validate_cross_file(manifest:, datasets_by_id:)
      errors = []
      combos = fetch(manifest, "combos", [])

      combos.each do |combo_entry|
        combo_id = fetch(combo_entry, "id")
        dataset = datasets_by_id[combo_id]

        unless dataset
          errors << error(
            code: "E_MESSAGE_COUNT_MISMATCH",
            message: "Missing dataset for combo #{combo_id}",
            path: "/datasets/#{combo_id}"
          )
          next
        end

        dataset_combo = fetch(dataset, "combo", {})
        unless fetch(dataset_combo, "id") == combo_id
          errors << error(
            code: "E_PROTOCOL_DECODE",
            message: "Combo id mismatch for #{combo_id}",
            path: "/datasets/#{combo_id}/combo/id"
          )
        end

        %w[protocol transport].each do |field|
          expected = fetch(combo_entry, field)
          actual = fetch(dataset_combo, field)
          next if expected == actual

          errors << error(
            code: "E_PROTOCOL_DECODE",
            message: "Combo #{field} mismatch for #{combo_id}: expected #{expected.inspect}, got #{actual.inspect}",
            path: "/datasets/#{combo_id}/combo/#{field}"
          )
        end

        expected_count = fetch(combo_entry, "message_count")
        metadata_count = fetch(fetch(dataset, "metadata", {}), "message_count")
        actual_count = Array(fetch(dataset, "messages", [])).length

        unless expected_count == metadata_count && metadata_count == actual_count
          errors << error(
            code: "E_MESSAGE_COUNT_MISMATCH",
            message: "Message count mismatch for #{combo_id}: manifest=#{expected_count.inspect}, metadata=#{metadata_count.inspect}, messages=#{actual_count}",
            path: "/datasets/#{combo_id}/messages"
          )
        end
      end

      Result.new(errors: errors)
    end

    def validate_all(manifest:, datasets_by_id:)
      errors = []
      errors.concat(validate_manifest(manifest).errors)
      datasets_by_id.each_value { |dataset| errors.concat(validate_dataset(dataset).errors) }
      errors.concat(validate_cross_file(manifest: manifest, datasets_by_id: datasets_by_id).errors)
      Result.new(errors: errors)
    end

    private

    def load_schema(filename)
      schema_path = @schema_dir.join(filename)
      schema_json = JSON.parse(File.read(schema_path))
      JSONSchemer.schema(schema_json)
    end

    def schema_errors(schemer, document, default_code:)
      schemer.validate(document).map do |entry|
        message = "#{entry['type']} at #{entry['data_pointer']}"
        error(code: default_code, message: message, path: entry["data_pointer"])
      end
    end

    def validate_message_count(dataset)
      metadata_count = fetch(fetch(dataset, "metadata", {}), "message_count")
      actual_count = Array(fetch(dataset, "messages", [])).length
      return [] if metadata_count == actual_count

      [
        error(
          code: "E_MESSAGE_COUNT_MISMATCH",
          message: "metadata.message_count=#{metadata_count.inspect} but messages.length=#{actual_count}",
          path: "/messages"
        )
      ]
    end

    def validate_message_indices(dataset)
      messages = Array(fetch(dataset, "messages", []))
      errors = []

      messages.each_with_index do |message, idx|
        actual = fetch(message, "index")
        next if actual == idx

        errors << error(
          code: "E_PROTOCOL_DECODE",
          message: "Message index mismatch at position #{idx}: got #{actual.inspect}",
          path: "/messages/#{idx}/index"
        )
      end

      errors
    end

    def validate_span_invariants(dataset)
      errors = []

      Array(fetch(dataset, "messages", [])).each_with_index do |message, index|
        raw_size = fetch(message, "raw_size", 0).to_i
        transport = fetch(message, "transport", {})
        payload_span = fetch(transport, "payload_span")
        envelope_span = fetch(fetch(message, "envelope", {}), "span")
        payload_body_span = fetch(fetch(message, "payload", {}), "span")

        unless valid_span?(payload_span, raw_size)
          errors << error(code: "E_PROTOCOL_DECODE", message: "Invalid transport.payload_span", path: "/messages/#{index}/transport/payload_span")
        end

        if valid_span?(payload_span, raw_size) && valid_span?(envelope_span, raw_size) && !contains_span?(payload_span, envelope_span)
          errors << error(code: "E_PROTOCOL_DECODE", message: "Envelope span outside transport payload span", path: "/messages/#{index}/envelope/span")
        end

        if valid_span?(payload_span, raw_size) && valid_span?(payload_body_span, raw_size) && !contains_span?(payload_span, payload_body_span)
          errors << error(code: "E_PROTOCOL_DECODE", message: "Payload span outside transport payload span", path: "/messages/#{index}/payload/span")
        end

        Array(fetch(fetch(message, "payload", {}), "fields", [])).each_with_index do |field, field_idx|
          validate_field_spans(field, payload_body_span, raw_size, errors, path: "/messages/#{index}/payload/fields/#{field_idx}")
        end
      end

      errors
    end

    def validate_field_spans(field, payload_span, raw_size, errors, path:)
      span = fetch(field, "span")
      unless valid_span?(span, raw_size)
        errors << error(code: "E_PROTOCOL_DECODE", message: "Invalid field span", path: "#{path}/span")
        return
      end

      if valid_span?(payload_span, raw_size) && !contains_span?(payload_span, span)
        errors << error(code: "E_PROTOCOL_DECODE", message: "Field span outside payload span", path: "#{path}/span")
      end

      Array(fetch(field, "children", [])).each_with_index do |child, idx|
        validate_field_spans(child, payload_span, raw_size, errors, path: "#{path}/children/#{idx}")
      end
    end

    def validate_field_ordering(dataset)
      errors = []

      Array(fetch(dataset, "messages", [])).each_with_index do |message, index|
        fields = Array(fetch(fetch(message, "payload", {}), "fields", []))
        validate_field_array_order(fields, errors, path: "/messages/#{index}/payload/fields")
      end

      errors
    end

    def validate_field_array_order(fields, errors, path:)
      previous = nil

      fields.each_with_index do |field, idx|
        if previous && (compare_field_order(previous, field) > 0)
          errors << error(
            code: "E_PROTOCOL_DECODE",
            message: "Field ordering is not canonical",
            path: "#{path}/#{idx}"
          )
          break
        end

        children = Array(fetch(field, "children", []))
        validate_field_array_order(children, errors, path: "#{path}/#{idx}/children")
        previous = field
      end
    end

    def compare_field_order(left, right)
      left_span = Array(fetch(left, "span", [0, 0]))
      right_span = Array(fetch(right, "span", [0, 0]))

      start_cmp = left_span[0].to_i <=> right_span[0].to_i
      return start_cmp unless start_cmp.zero?

      end_cmp = right_span[1].to_i <=> left_span[1].to_i
      return end_cmp unless end_cmp.zero?

      left_id = fetch(left, "id")
      right_id = fetch(right, "id")
      id_cmp = compare_field_id(left_id, right_id)
      return id_cmp unless id_cmp.zero?

      fetch(left, "name", "").to_s <=> fetch(right, "name", "").to_s
    end

    def compare_field_id(left, right)
      left_numeric = left.is_a?(Integer)
      right_numeric = right.is_a?(Integer)

      if left_numeric && right_numeric
        left <=> right
      elsif left_numeric
        -1
      elsif right_numeric
        1
      else
        left.to_s <=> right.to_s
      end
    end

    def validate_tutorial_flow(dataset)
      client_calls = Array(fetch(dataset, "messages", [])).select do |message|
        fetch(message, "actor") == "client" && %w[call oneway].include?(fetch(message, "message_type"))
      end

      sequence = client_calls.map { |message| fetch(message, "method") }
      errors = []

      unless sequence == EXPECTED_CALL_SEQUENCE
        errors << error(
          code: "E_TUTORIAL_FLOW_MISMATCH",
          message: "Tutorial method sequence mismatch: expected #{EXPECTED_CALL_SEQUENCE.inspect}, got #{sequence.inspect}",
          path: "/messages"
        )
      end

      zip_call = client_calls.last
      if zip_call && (fetch(zip_call, "method") != "zip" || fetch(zip_call, "message_type") != "oneway")
        errors << error(
          code: "E_TUTORIAL_FLOW_MISMATCH",
          message: "zip must be sent as oneway",
          path: "/messages/#{fetch(zip_call, 'index', 0)}"
        )
      end

      errors
    end

    def valid_span?(span, raw_size)
      return false unless span.is_a?(Array) && span.length == 2

      start_pos = span[0]
      end_pos = span[1]
      return false unless start_pos.is_a?(Integer) && end_pos.is_a?(Integer)
      return false unless start_pos >= 0 && end_pos > start_pos

      end_pos <= raw_size
    end

    def contains_span?(outer, inner)
      outer_start, outer_end = outer
      inner_start, inner_end = inner
      inner_start >= outer_start && inner_end <= outer_end
    end

    def fetch(hash_like, key, default = nil)
      return default unless hash_like.respond_to?(:[])

      if hash_like.respond_to?(:key?)
        return hash_like[key] if hash_like.key?(key)
        return hash_like[key.to_s] if hash_like.key?(key.to_s)
        return hash_like[key.to_sym] if hash_like.key?(key.to_sym)
      end

      default
    end

    def error(code:, message:, path:)
      {
        code: code,
        message: message,
        path: path,
        severity: "fatal"
      }
    end
  end
end
