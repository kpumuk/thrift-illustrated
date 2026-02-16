# frozen_string_literal: true

module ThriftIllustrated
  module ThriftClientSeqidPatch
    def send_message(name, args_class, args = {})
      @oprot.write_message_begin(name, Thrift::MessageTypes::CALL, next_sequence_id!)
      send_message_args(args_class, args)
    end

    def send_oneway_message(name, args_class, args = {})
      @oprot.write_message_begin(name, Thrift::MessageTypes::ONEWAY, next_sequence_id!)
      send_message_args(args_class, args)
    end

    private

    # Ruby thrift runtime currently leaves @seqid fixed at 0; bump per request.
    def next_sequence_id!
      @seqid = @seqid.to_i + 1
    end
  end
end

unless Thrift::Client.ancestors.include?(ThriftIllustrated::ThriftClientSeqidPatch)
  Thrift::Client.prepend(ThriftIllustrated::ThriftClientSeqidPatch)
end
