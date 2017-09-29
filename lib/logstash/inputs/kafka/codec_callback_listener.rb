# encoding: utf-8
require "logstash/inputs/kafka"

module LogStash module Inputs class Kafka
  # Use the new callback based approch instead of using blocks
  # so we can retain some context of the execution, and make it easier to test
  class CodecCallbackListener
    attr_accessor :data
    # The path acts as the `stream_identity`, 
    # usefull when the clients is reading multiples files
    attr_accessor :path

    def initialize(record, input, queue, decorate_events, group_id)
      @data = record.value.to_s
      @path = record.key
      @record = record
      @input = input
      @queue = queue
      @decorate_events = decorate_events
      @group_id = group_id
    end

    def process_event(event)
      @input.send(:decorate, event)
      if @decorate_events
        event.set("[@metadata][kafka][topic]", @record.topic)
        event.set("[@metadata][kafka][consumer_group]", @group_id)
        event.set("[@metadata][kafka][partition]", @record.partition)
        event.set("[@metadata][kafka][offset]", @record.offset)
        event.set("[@metadata][kafka][key]", @record.key)
        event.set("[@metadata][kafka][timestamp]", @record.timestamp)
      end
      @queue << event
    end
  end
end; end; end