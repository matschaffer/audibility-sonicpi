#!/usr/bin/env ruby
# frozen_string_literal: true

require 'net/http'
require 'json'

CLIENT = Net::HTTP.new('localhost', 9200)

def req(method, path = '/', body = {})
  request = method.new(path)
  request.set_content_type('application/json')
  request.body = body.to_json
  p body
  CLIENT.request(request)
end

def wait_for_es
  req(Net::HTTP::Get)
rescue Errno::ECONNREFUSED, EOFError
  sleep 1
  retry
end

def put_pipeline
  puts 'Creating pipeline'
  puts req(Net::HTTP::Put, '/_ingest/pipeline/timestamp',
           description: 'Adds @timestamp to documents',
           processors: [
             { set: { field: '@timestamp', value: '{{ _ingest.timestamp }}' } }
           ]).body
end

def put_index(index)
  puts 'Creating index'
  puts req(Net::HTTP::Put, "/#{index}",
           settings: {
             index: {
               default_pipeline: 'timestamp'
             }
           }).body
end

wait_for_es

index = ARGV[0] || 'notes'

put_pipeline
put_index(index)

base_note = ARGV[1].to_i || 60
top_note = ARGV[2].to_i || 72
interval = ARGV[3].to_f || 0.5

note = base_note
loop do
  req(Net::HTTP::Post, "/#{index}/_doc",
      note: note)
  note += 1
  note = base_note if note > top_note
  sleep interval
end
