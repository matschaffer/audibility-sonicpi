require 'net/http'
require 'json'
require 'time'

CLIENT = Net::HTTP.new('localhost', 9200)

def req(method, path = '/', body = {})
  request = method.new(path)
  request.set_content_type('application/json')
  request.body = body.to_json
  CLIENT.request(request)
end

@delay = 10
@initial_load = 40
@block_size = 10
@size = 100
@blocks = []

live_loop :loader do
  if @blocks.empty?
    puts "Initial load"
    initial_query = {
      size: 0,
      query: {
        bool: {
          filter: [
            {range: {'@timestamp': {gte: "now-#{@initial_load}s"}}},
            {range: {'@timestamp': {lt: "now-#{@delay}s"}}}
          ]
        }
      },
      aggs: {
        blocks: {
          date_histogram: {
            field: "@timestamp",
            fixed_interval: "#{@block_size}s",
          },
          aggs: {
            docs: {
              top_hits: {size: @size}
            }
          }
        }
      }
    }

    res = JSON.parse(req(Net::HTTP::Get, '/notes/_search', initial_query).body)
    @blocks += res['aggregations']['blocks']['buckets'][0..-1].map { |block|
      {
        key: block['key'],
        events: block['docs']['hits']['hits'].map { |doc|
          doc['_source']
        }
      }
    }
    sleep @block_size
  else
    last_block_start = Time.at(@blocks.last[:key] / 1000)
    if Time.now - last_block_start > @block_size + @delay
      puts "Loading next block"
      next_block_start = last_block_start + @block_size
      next_query = {
        size: @size,
        query: {
          bool: {
            filter: [
              {range: {'@timestamp': {gte: next_block_start.iso8601}}},
              {range: {'@timestamp': {lt: (next_block_start + @block_size).iso8601}}}
            ]
          }
        }
      }
      res = JSON.parse(req(Net::HTTP::Get, '/notes/_search', next_query).body)
      @blocks.push({
                     key: next_block_start.to_i * 1000,
                     events: res['hits']['hits'].map { |doc|
                       doc['_source']
                     }
      })
    end
  end
  sleep @block_size
end

live_loop :player do
  block = @blocks.shift
  if block
    start = Time.at(block[:key] / 1000)
    block[:events].each do |event|
      at Time.iso8601(event['@timestamp']) - start do
        play event['note']
      end
    end
    sleep @block_size
  else
    sleep 1
  end
end
