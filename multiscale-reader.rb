require 'net/http'
require 'json'
require 'time'

INITIAL_LOAD = 40
DELAY = 10
BLOCK_DURATION = 10
SIZE = 100

def es_req(client, path, body)
  request = Net::HTTP::Get.new(path)
  request.set_content_type('application/json')
  request.body = body.to_json
  JSON.parse(client.request(request).body)
end

def es_player(index_pattern, host = 'localhost', port = 9200)
  client = Net::HTTP.new(host, port)
  blocks = []

  live_loop "loader: #{index_pattern}" do
    if blocks.empty?
      puts "Initial load"
      initial_query = {
        size: 0,
        query: {
          bool: {
            filter: [
              {range: {'@timestamp': {gte: "now-#{INITIAL_LOAD}s"}}},
              {range: {'@timestamp': {lt: "now-#{DELAY}s"}}}
            ]
          }
        },
        aggs: {
          blocks: {
            date_histogram: {
              field: "@timestamp",
              fixed_interval: "#{BLOCK_DURATION}s",
            },
            aggs: {
              docs: {
                top_hits: {size: SIZE}
              }
            }
          }
        }
      }

      res = es_req(client, "/#{index_pattern}/_search", initial_query)
      blocks += res['aggregations']['blocks']['buckets'][0..-1].map { |block|
        {
          key: block['key'],
          events: block['docs']['hits']['hits'].map { |doc|
            doc['_source']
          }
        }
      }
      puts "Loaded #{blocks.length} blocks for #{index_pattern}"
    else
      last_block_start = Time.at(blocks.last[:key] / 1000)
      if Time.now - last_block_start > BLOCK_DURATION + DELAY
        puts "Loading next block"
        next_block_start = last_block_start + BLOCK_DURATION
        next_query = {
          size: SIZE,
          query: {
            bool: {
              filter: [
                {range: {'@timestamp': {gte: next_block_start.iso8601}}},
                {range: {'@timestamp': {lt: (next_block_start + BLOCK_DURATION).iso8601}}}
              ]
            }
          }
        }
        res = es_req(client, "/#{index_pattern}/_search", next_query)
        blocks.push({
                      key: next_block_start.to_i * 1000,
                      events: res['hits']['hits'].map { |doc|
                        doc['_source']
                      }
        })
        puts "Loaded #{blocks.length} blocks for #{index_pattern}"
      end
    end
    sleep 1
  end

  live_loop "player #{index_pattern}" do
    block = blocks.shift
    if block
      start = Time.at(block[:key] / 1000)
      block[:events].each do |event|
        at Time.iso8601(event['@timestamp']) - start do
          yield event
        end
      end
      sleep BLOCK_DURATION
    else
      sleep 1
    end
  end
end

es_player('notes') do |event|
  play event['note']
end

es_player('saws') do |event|
  use_synth :saw
  play event['note']
end
