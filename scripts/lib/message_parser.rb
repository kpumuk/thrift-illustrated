# frozen_string_literal: true

require "base64"
require "thrift"

module ThriftIllustrated
  class MessageParser
    TYPE_NAMES = {
      Thrift::Types::STOP => "stop",
      Thrift::Types::BOOL => "bool",
      Thrift::Types::BYTE => "byte",
      Thrift::Types::DOUBLE => "double",
      Thrift::Types::I16 => "i16",
      Thrift::Types::I32 => "i32",
      Thrift::Types::I64 => "i64",
      Thrift::Types::STRING => "string",
      Thrift::Types::STRUCT => "struct",
      Thrift::Types::MAP => "map",
      Thrift::Types::SET => "set",
      Thrift::Types::LIST => "list",
      Thrift::Types::UUID => "uuid"
    }.freeze

    MESSAGE_TYPES = {
      Thrift::MessageTypes::CALL => "call",
      Thrift::MessageTypes::REPLY => "reply",
      Thrift::MessageTypes::EXCEPTION => "exception",
      Thrift::MessageTypes::ONEWAY => "oneway"
    }.freeze

    DEFAULT_LIMITS = {
      max_field_nodes: 5000,
      max_recursion_depth: 64,
      max_string_bytes: 65_536,
      max_highlights: 1000
    }.freeze
    GENERATED_STUB_DIR = File.expand_path("../../thrift/gen-rb", __dir__)

    class LimitExceeded < StandardError
      attr_reader :code, :offset, :severity

      def initialize(message, code:, offset:, severity: "fatal")
        super(message)
        @code = code
        @offset = offset
        @severity = severity
      end
    end

    class CursorTransport < Thrift::BaseTransport
      attr_reader :position

      def initialize(buffer)
        @buffer = buffer.b
        @position = 0
      end

      def read(size)
        raise EOFError, "Not enough bytes remain in buffer" if @position + size > @buffer.bytesize

        chunk = @buffer.byteslice(@position, size)
        @position += size
        chunk
      end

      def available
        @buffer.bytesize - @position
      end
    end

    def initialize(
      max_field_nodes: DEFAULT_LIMITS[:max_field_nodes],
      max_recursion_depth: DEFAULT_LIMITS[:max_recursion_depth],
      max_string_bytes: DEFAULT_LIMITS[:max_string_bytes],
      max_highlights: DEFAULT_LIMITS[:max_highlights]
    )
      @max_field_nodes = max_field_nodes
      @max_recursion_depth = max_recursion_depth
      @max_string_bytes = max_string_bytes
      @max_highlights = max_highlights
      @message_struct_registry = build_message_struct_registry
    end

    def parse_records(records:, protocol:, transport:, actor:, starting_index: 0)
      sorted = Array(records).sort_by { |entry| record_chunk_index(entry) }
      payload = sorted.map { |entry| Base64.strict_decode64(record_bytes_base64(entry)) }.join

      parse_bytes(
        raw_bytes: payload,
        protocol: protocol,
        transport: transport,
        actor: actor,
        wire_chunk_count: sorted.length,
        starting_index: starting_index
      )
    end

    def parse_bytes(raw_bytes:, protocol:, transport:, actor:, wire_chunk_count: 0, starting_index: 0)
      bytes = raw_bytes.to_s.b
      messages =
        if transport.to_s == "framed"
          parse_framed_stream(bytes, protocol: protocol, actor: actor, starting_index: starting_index)
        else
          parse_buffered_stream(bytes, protocol: protocol, actor: actor, starting_index: starting_index)
        end

      {
        messages: messages,
        parse_errors: messages.flat_map { |message| message.fetch(:parse_errors) },
        wire_chunk_count: wire_chunk_count,
        max_field_depth: messages.map { |message| message.fetch(:_max_field_depth, 0) }.max || 0,
        total_field_nodes: messages.sum { |message| message.fetch(:_field_node_count, 0) },
        max_string_value_bytes: messages.map { |message| message.fetch(:_max_string_value_bytes, 0) }.max || 0,
        max_highlights_per_message: @max_highlights
      }
    end

    private

    def parse_buffered_stream(bytes, protocol:, actor:, starting_index:)
      direction = actor.to_s == "client" ? "client->server" : "server->client"
      messages = []
      offset = 0
      index = starting_index

      while offset < bytes.bytesize
        parsed = parse_message_payload(
          payload_bytes: bytes.byteslice(offset, bytes.bytesize - offset),
          protocol: protocol,
          actor: actor,
          direction: direction,
          transport: "buffered",
          index: index,
          payload_offset: 0,
          frame_length: nil,
          frame_header_span: nil
        )

        consumed = parsed.fetch(:consumed)
        break if consumed <= 0

        raw = bytes.byteslice(offset, consumed)
        messages << build_message(raw: raw, parsed: parsed)

        offset += consumed
        index += 1
      end

      if offset < bytes.bytesize
        raw = bytes.byteslice(offset, bytes.bytesize - offset)
        messages << synthetic_error_message(
          raw: raw,
          index: index,
          actor: actor,
          direction: direction,
          transport: "buffered",
          payload_span: [0, raw.bytesize],
          error: parse_error(
            code: "E_PROTOCOL_DECODE",
            message: "Unparsed bytes remain in buffered stream",
            offset: 0,
            severity: "error",
            span: nil
          )
        )
      end

      messages
    end

    def parse_framed_stream(bytes, protocol:, actor:, starting_index:)
      direction = actor.to_s == "client" ? "client->server" : "server->client"
      messages = []
      offset = 0
      index = starting_index

      while offset < bytes.bytesize
        if bytes.bytesize - offset < 4
          raw = bytes.byteslice(offset, bytes.bytesize - offset)
          messages << synthetic_error_message(
            raw: raw,
            index: index,
            actor: actor,
            direction: direction,
            transport: "framed",
            payload_span: safe_span(start: 0, finish: raw.bytesize, max_end: raw.bytesize),
            frame_length: nil,
            frame_header_span: safe_span(start: 0, finish: [raw.bytesize, 4].min, max_end: raw.bytesize),
            error: parse_error(
              code: "E_FRAME_TRUNCATED",
              message: "Frame header is truncated",
              offset: 0,
              severity: "fatal",
              span: [0, raw.bytesize]
            )
          )
          break
        end

        frame_length = bytes.byteslice(offset, 4).unpack1("l>")
        if frame_length.negative?
          raw = bytes.byteslice(offset, 4)
          messages << synthetic_error_message(
            raw: raw,
            index: index,
            actor: actor,
            direction: direction,
            transport: "framed",
            payload_span: safe_span(start: 0, finish: raw.bytesize, max_end: raw.bytesize),
            frame_length: frame_length,
            frame_header_span: safe_span(start: 0, finish: 4, max_end: raw.bytesize),
            error: parse_error(
              code: "E_FRAME_LENGTH_MISMATCH",
              message: "Negative frame length #{frame_length}",
              offset: 0,
              severity: "error",
              span: [0, 4]
            )
          )
          offset += 4
          index += 1
          next
        end

        total_size = frame_length + 4
        if offset + total_size > bytes.bytesize
          raw = bytes.byteslice(offset, bytes.bytesize - offset)
          messages << synthetic_error_message(
            raw: raw,
            index: index,
            actor: actor,
            direction: direction,
            transport: "framed",
            payload_span: safe_span(start: 4, finish: raw.bytesize, max_end: raw.bytesize),
            frame_length: frame_length,
            frame_header_span: safe_span(start: 0, finish: 4, max_end: raw.bytesize),
            error: parse_error(
              code: "E_FRAME_TRUNCATED",
              message: "Frame payload truncated: expected #{frame_length} bytes",
              offset: 4,
              severity: "fatal",
              span: [4, raw.bytesize]
            )
          )
          break
        end

        raw = bytes.byteslice(offset, total_size)
        payload = raw.byteslice(4, frame_length)

        parsed = parse_message_payload(
          payload_bytes: payload,
          protocol: protocol,
          actor: actor,
          direction: direction,
          transport: "framed",
          index: index,
          payload_offset: 4,
          frame_length: frame_length,
          frame_header_span: [0, 4]
        )

        messages << build_message(raw: raw, parsed: parsed)

        offset += total_size
        index += 1
      end

      messages
    end

    def parse_message_payload(payload_bytes:, protocol:, actor:, direction:, transport:, index:, payload_offset:, frame_length:, frame_header_span:)
      reader = CursorTransport.new(payload_bytes)
      thrift_protocol = build_protocol(protocol, reader)
      parse_errors = []
      stats = { field_nodes: 0, max_depth: 0, max_string_bytes: 0 }

      envelope_start = reader.position
      name = "__parse_error__"
      message_type = "call"
      seqid = 0
      envelope_end = envelope_start
      payload_start = envelope_start
      payload_end = envelope_start
      fields = []

      begin
        name, message_type_code, seqid = thrift_protocol.read_message_begin
        message_type = MESSAGE_TYPES.fetch(message_type_code, "call")
        envelope_end = reader.position
        payload_start = reader.position

        thrift_protocol.read_struct_begin
        root_struct_class = resolve_message_struct_class(method: name, message_type: message_type)
        fields = read_struct_fields(
          protocol: thrift_protocol,
          reader: reader,
          depth: 0,
          stats: stats,
          parse_errors: parse_errors,
          struct_class: root_struct_class
        )
        thrift_protocol.read_struct_end
        thrift_protocol.read_message_end
        payload_end = reader.position

        if transport == "framed" && reader.position != payload_bytes.bytesize
          parse_errors << parse_error(
            code: "E_PROTOCOL_DECODE",
            message: "Message parser did not consume full payload",
            offset: payload_offset + reader.position,
            severity: "warning",
            span: nil
          )
        end
      rescue LimitExceeded => e
        parse_errors << parse_error(
          code: e.code,
          message: e.message,
          offset: payload_offset + e.offset,
          severity: e.severity,
          span: nil
        )
        payload_end = reader.position
      rescue EOFError => e
        parse_errors << parse_error(
          code: "E_PROTOCOL_DECODE",
          message: e.message,
          offset: payload_offset + reader.position,
          severity: "error",
          span: nil
        )
        payload_end = reader.position
      rescue StandardError => e
        parse_errors << parse_error(
          code: "E_PROTOCOL_DECODE",
          message: e.message,
          offset: payload_offset + reader.position,
          severity: "error",
          span: nil
        )
        payload_end = reader.position
      end

      consumed = reader.position
      adjusted_fields = payload_offset.zero? ? fields : shift_field_spans(fields, payload_offset)
      payload_span =
        if transport == "framed"
          safe_span(start: 4, finish: payload_offset + payload_bytes.bytesize, max_end: payload_offset + payload_bytes.bytesize)
        else
          safe_span(start: 0, finish: consumed, max_end: consumed)
        end

      {
        index: index,
        actor: actor.to_s,
        direction: direction,
        method: name.to_s.empty? ? "__parse_error__" : name,
        message_type: message_type,
        seqid: seqid,
        raw_size: nil,
        transport: {
          type: transport,
          frame_length: frame_length,
          frame_header_span: frame_header_span,
          payload_span: payload_span
        },
        envelope: {
          name: name.to_s.empty? ? "__parse_error__" : name,
          type: message_type,
          seqid: seqid,
          span: safe_span(start: payload_offset + envelope_start, finish: payload_offset + envelope_end, max_end: payload_offset + payload_bytes.bytesize)
        },
        payload: {
          struct: {
            name: "args",
            ttype: "struct",
            span: safe_span(start: payload_offset + payload_start, finish: payload_offset + payload_end, max_end: payload_offset + payload_bytes.bytesize)
          },
          span: safe_span(start: payload_offset + payload_start, finish: payload_offset + payload_end, max_end: payload_offset + payload_bytes.bytesize),
          fields: adjusted_fields
        },
        highlights: [],
        parse_errors: parse_errors,
        consumed: transport == "framed" ? payload_bytes.bytesize : consumed,
        _field_node_count: stats[:field_nodes],
        _max_field_depth: stats[:max_depth],
        _max_string_value_bytes: stats[:max_string_bytes]
      }
    end

    def build_message(raw:, parsed:)
      message = parsed.dup
      message.delete(:consumed)
      message[:raw_size] = raw.bytesize
      message[:raw_hex] = bytes_to_hex(raw)
      message
    end

    def synthetic_error_message(raw:, index:, actor:, direction:, transport:, payload_span:, error:, frame_length: nil, frame_header_span: nil)
      {
        index: index,
        actor: actor.to_s,
        direction: direction,
        method: "__parse_error__",
        message_type: "call",
        seqid: 0,
        raw_hex: bytes_to_hex(raw),
        raw_size: raw.bytesize,
        transport: {
          type: transport,
          frame_length: frame_length,
          frame_header_span: frame_header_span,
          payload_span: payload_span
        },
        envelope: {
          name: "__parse_error__",
          type: "call",
          seqid: 0,
          span: payload_span
        },
        payload: {
          struct: {
            name: "args",
            ttype: "struct",
            span: payload_span
          },
          span: payload_span,
          fields: []
        },
        highlights: [],
        parse_errors: [error],
        _field_node_count: 0,
        _max_field_depth: 0,
        _max_string_value_bytes: 0
      }
    end

    def read_struct_fields(protocol:, reader:, depth:, stats:, parse_errors:, struct_class:)
      ensure_depth!(depth: depth, offset: reader.position)
      stats[:max_depth] = [stats[:max_depth], depth].max

      fields = []
      loop do
        field_start = reader.position
        _name, field_type, field_id = protocol.read_field_begin
        break if field_type == Thrift::Types::STOP
        field_schema = field_schema_for(struct_class, field_id)

        value = read_typed_value(
          protocol: protocol,
          reader: reader,
          type: field_type,
          depth: depth + 1,
          stats: stats,
          parse_errors: parse_errors,
          element_id: field_id,
          schema: field_schema
        )
        field_end = reader.position

        protocol.read_field_end

        fields << build_field_node(
          id: field_id,
          name: field_name_for(field_id: field_id, field_schema: field_schema),
          ttype: type_name(field_type),
          span: [field_start, field_end],
          value: value[:value],
          children: value[:children]
        )
        increment_field_count!(stats: stats, offset: field_end)
      end

      fields
    end

    def read_typed_value(protocol:, reader:, type:, depth:, stats:, parse_errors:, element_id:, schema:)
      ensure_depth!(depth: depth, offset: reader.position)
      stats[:max_depth] = [stats[:max_depth], depth].max

      case type
      when Thrift::Types::BOOL
        { value: protocol.read_bool, children: [] }
      when Thrift::Types::BYTE
        { value: protocol.read_byte, children: [] }
      when Thrift::Types::I16
        { value: protocol.read_i16, children: [] }
      when Thrift::Types::I32
        { value: protocol.read_i32, children: [] }
      when Thrift::Types::I64
        { value: protocol.read_i64, children: [] }
      when Thrift::Types::DOUBLE
        { value: protocol.read_double, children: [] }
      when Thrift::Types::STRING
        str = protocol.read_string
        string_size = str.to_s.bytesize
        stats[:max_string_bytes] = [stats[:max_string_bytes], string_size].max
        if string_size > @max_string_bytes
          raise LimitExceeded.new(
            "String field exceeds max decoded bytes (#{string_size} > #{@max_string_bytes})",
            code: "E_STRING_TOO_LARGE",
            offset: reader.position,
            severity: "fatal"
          )
        end
        { value: str, children: [] }
      when Thrift::Types::UUID
        { value: protocol.read_uuid, children: [] }
      when Thrift::Types::STRUCT
        struct_start = reader.position
        protocol.read_struct_begin
        child_struct_class = schema.is_a?(Hash) ? schema[:class] : nil
        children = read_struct_fields(
          protocol: protocol,
          reader: reader,
          depth: depth + 1,
          stats: stats,
          parse_errors: parse_errors,
          struct_class: child_struct_class
        )
        protocol.read_struct_end
        struct_end = reader.position

        { value: nil, children: [build_field_node(id: element_id, name: "struct", ttype: "struct", span: [struct_start, struct_end], value: nil, children: children)] }
      when Thrift::Types::LIST
        etype, size = protocol.read_list_begin
        element_schema = schema.is_a?(Hash) ? schema[:element] : nil
        children = read_collection_children(
          protocol: protocol,
          reader: reader,
          child_type: etype,
          size: size,
          depth: depth + 1,
          stats: stats,
          parse_errors: parse_errors,
          label: "list",
          item_schema: element_schema
        )
        protocol.read_list_end
        { value: nil, children: children }
      when Thrift::Types::SET
        etype, size = protocol.read_set_begin
        element_schema = schema.is_a?(Hash) ? schema[:element] : nil
        children = read_collection_children(
          protocol: protocol,
          reader: reader,
          child_type: etype,
          size: size,
          depth: depth + 1,
          stats: stats,
          parse_errors: parse_errors,
          label: "set",
          item_schema: element_schema
        )
        protocol.read_set_end
        { value: nil, children: children }
      when Thrift::Types::MAP
        key_type, value_type, size = protocol.read_map_begin
        key_schema = schema.is_a?(Hash) ? schema[:key] : nil
        value_schema = schema.is_a?(Hash) ? schema[:value] : nil
        children = []

        size.times do |entry_index|
          entry_start = reader.position
          key_value = read_typed_value(
            protocol: protocol,
            reader: reader,
            type: key_type,
            depth: depth + 1,
            stats: stats,
            parse_errors: parse_errors,
            element_id: "key",
            schema: key_schema
          )
          mapped_value = read_typed_value(
            protocol: protocol,
            reader: reader,
            type: value_type,
            depth: depth + 1,
            stats: stats,
            parse_errors: parse_errors,
            element_id: "value",
            schema: value_schema
          )
          entry_end = reader.position

          key_node = build_field_node(
            id: "key",
            name: "key",
            ttype: type_name(key_type),
            span: [entry_start, entry_end],
            value: key_value[:value],
            children: key_value[:children]
          )
          value_node = build_field_node(
            id: "value",
            name: "value",
            ttype: type_name(value_type),
            span: [entry_start, entry_end],
            value: mapped_value[:value],
            children: mapped_value[:children]
          )

          children << build_field_node(
            id: entry_index,
            name: "entry_#{entry_index}",
            ttype: "map_entry",
            span: [entry_start, entry_end],
            value: nil,
            children: [key_node, value_node]
          )
          increment_field_count!(stats: stats, offset: entry_end)
        end

        protocol.read_map_end
        { value: nil, children: children }
      else
        start = reader.position
        parse_errors << parse_error(
          code: "E_FIELD_TYPE_UNKNOWN",
          message: "Unknown field type #{type}",
          offset: start,
          severity: "error",
          span: nil
        )
        protocol.skip(type)
        finish = reader.position
        { value: nil, children: [build_field_node(id: element_id, name: "unknown", ttype: type_name(type), span: [start, finish], value: nil, children: [])] }
      end
    end

    def read_collection_children(protocol:, reader:, child_type:, size:, depth:, stats:, parse_errors:, label:, item_schema:)
      children = []
      size.times do |index|
        child_start = reader.position
        parsed = read_typed_value(
          protocol: protocol,
          reader: reader,
          type: child_type,
          depth: depth,
          stats: stats,
          parse_errors: parse_errors,
          element_id: index,
          schema: item_schema
        )
        child_end = reader.position

        children << build_field_node(
          id: index,
          name: "#{label}_#{index}",
          ttype: type_name(child_type),
          span: [child_start, child_end],
          value: parsed[:value],
          children: parsed[:children]
        )
        increment_field_count!(stats: stats, offset: child_end)
      end
      children
    end

    def resolve_message_struct_class(method:, message_type:)
      @message_struct_registry[[method.to_s, message_type.to_s]]
    end

    def field_schema_for(struct_class, field_id)
      return nil unless struct_class && field_id.is_a?(Integer)
      return nil unless struct_class.const_defined?(:FIELDS)

      fields = struct_class.const_get(:FIELDS)
      fields[field_id]
    rescue StandardError
      nil
    end

    def field_name_for(field_id:, field_schema:)
      schema_name = field_schema.is_a?(Hash) ? field_schema[:name] : nil
      return schema_name.to_s unless schema_name.to_s.empty?

      "field_#{field_id}"
    end

    def build_message_struct_registry
      return {} unless File.file?(File.join(GENERATED_STUB_DIR, "calculator.rb"))

      $LOAD_PATH.unshift(GENERATED_STUB_DIR) unless $LOAD_PATH.include?(GENERATED_STUB_DIR)
      require "calculator"
      require "shared_service"

      registry = {}
      register_message_struct(registry, "ping", "call", Calculator::Ping_args)
      register_message_struct(registry, "add", "call", Calculator::Add_args)
      register_message_struct(registry, "calculate", "call", Calculator::Calculate_args)
      register_message_struct(registry, "zip", "oneway", Calculator::Zip_args)
      register_message_struct(registry, "ping", "reply", Calculator::Ping_result)
      register_message_struct(registry, "add", "reply", Calculator::Add_result)
      register_message_struct(registry, "calculate", "reply", Calculator::Calculate_result)
      register_message_struct(registry, "zip", "reply", Calculator::Zip_result)
      register_message_struct(registry, "getStruct", "call", SharedService::GetStruct_args)
      register_message_struct(registry, "getStruct", "reply", SharedService::GetStruct_result)
      registry
    rescue LoadError, StandardError
      {}
    end

    def register_message_struct(registry, method, message_type, struct_class)
      registry[[method, message_type]] = struct_class if struct_class
    end

    def build_field_node(id:, name:, ttype:, span:, value:, children:)
      {
        id: id,
        name: name,
        ttype: ttype,
        span: span,
        value: value,
        children: children
      }
    end

    def build_protocol(protocol_name, transport)
      case protocol_name.to_s
      when "binary"
        Thrift::BinaryProtocol.new(transport)
      when "compact"
        Thrift::CompactProtocol.new(transport)
      when "json"
        Thrift::JsonProtocol.new(transport)
      else
        raise ArgumentError, "Unsupported protocol: #{protocol_name}"
      end
    end

    def type_name(type)
      TYPE_NAMES.fetch(type, "ttype_unknown_#{type}")
    end

    def parse_error(code:, message:, offset:, severity:, span:)
      {
        code: code,
        message: message,
        offset: offset,
        severity: severity,
        span: span
      }
    end

    def increment_field_count!(stats:, offset:)
      stats[:field_nodes] += 1
      return if stats[:field_nodes] <= @max_field_nodes

      raise LimitExceeded.new(
        "Field node limit exceeded (#{stats[:field_nodes]} > #{@max_field_nodes})",
        code: "E_FIELD_NODE_LIMIT",
        offset: offset,
        severity: "fatal"
      )
    end

    def ensure_depth!(depth:, offset:)
      return if depth <= @max_recursion_depth

      raise LimitExceeded.new(
        "Recursion depth limit exceeded (#{depth} > #{@max_recursion_depth})",
        code: "E_RECURSION_LIMIT",
        offset: offset,
        severity: "fatal"
      )
    end

    def bytes_to_hex(bytes)
      bytes.to_s.bytes.map { |byte| format("%02x", byte) }.join(" ")
    end

    def shift_field_spans(fields, delta)
      Array(fields).map do |field|
        shifted = field.dup
        shifted[:span] = shift_span(field[:span], delta)
        shifted[:children] = shift_field_spans(field[:children], delta)
        shifted
      end
    end

    def shift_span(span, delta)
      return span unless span.is_a?(Array) && span.length == 2

      [span[0].to_i + delta, span[1].to_i + delta]
    end

    def safe_span(start:, finish:, max_end: nil)
      s = [start.to_i, 0].max
      f = [finish.to_i, 0].max
      max_end = max_end.nil? ? nil : [max_end.to_i, 0].max

      if max_end
        s = [s, max_end].min
        f = [f, max_end].min
      end

      if f <= s
        if max_end && max_end.positive?
          s = [s, max_end - 1].min
          f = [s + 1, max_end].min
        else
          s = 0
          f = 1
        end
      end

      [s, f]
    end

    def record_chunk_index(entry)
      read_record_key(entry, :chunk_index)
    end

    def record_bytes_base64(entry)
      read_record_key(entry, :bytes_base64)
    end

    def read_record_key(entry, key)
      return entry.fetch(key) if entry.respond_to?(:fetch) && entry.key?(key)
      return entry.fetch(key.to_s) if entry.respond_to?(:fetch) && entry.key?(key.to_s)

      raise ArgumentError, "Record missing #{key}"
    end
  end
end
