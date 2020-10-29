@events = []

def too_new?(event)
  now = Time.now
  threshold = now - 5
  puts "checking event: #{event} at #{now}, gt #{threshold}"
  event.nil? or event['@timestamp'] > threshold
end

live_loop :filler do
  head = @events.first
  now = Time.now
  if too_new?(head)
    puts "Had #{@events.length} events, filling"
    30.times do |i|
      @events.append({
                       '@timestamp' => (now - 10) - (30 - i),
                       'note' => 60 + i
                     })
    end
    puts @events
  end
  sleep 1
end

def play_at(s, event)
  if s < 0
    puts "Skipping #{event} as too old"
  else
    puts "scheduling #{event} in #{s} beats"
    at s do
      puts "playing #{event}"
      play event['note']
    end
  end
end

live_loop :reader do
  now = Time.now

  play_start = now - 30
  play_end = now - 20

  puts "Playing from #{play_start} to #{play_end}"

  while @events.first && @events.first['@timestamp'] < play_end do
    event = @events.shift
    play_at(event['@timestamp'] - play_start, event)
  end
  sleep 2
end
