# frozen_string_literal: true

require 'delivery_boy'
require 'logger'
require 'typhoeus'

SOURCE_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
TOPIC = 'article-change-events'
CLIENT_ID = 'article-change-events-source'
KAFKA_BROKERS = ['docker.for.mac.localhost:9092'].freeze

def config_from_env
  { source_url: SOURCE_URL,
    topic: TOPIC,
    client_id: CLIENT_ID,
    brokers: KAFKA_BROKERS }
end

def to_data(raw_line)
  return nil unless raw_line.start_with? 'data'

  # Remove whitespace; it can cause problems.
  stripped_line = raw_line.strip

  # Skip objects that are split across multiple chunks.
  return nil unless stripped_line.end_with?('}')

  # Remove the prefix 'data: '
  stripped_line[6..-1]
end

def retrieve_events(source_url)
  req = Typhoeus::Request.new source_url

  req.on_headers { |response| raise 'Request failed' if response.code != 200 }

  req.on_body do |chunk|
    chunk.each_line do |raw_line|
      data = to_data raw_line
      yield data if data
    end
  end

  req.run
end

def produce_event(event, topic)
  DeliveryBoy.deliver event, topic: topic
end

def init_producer(config)
  DeliveryBoy.configure do |db_config|
    db_config.client_id = config.fetch :client_id
    db_config.brokers = config.fetch :brokers
  end

  logger = Logger.new $stdout
  logger.level = Logger::INFO
  DeliveryBoy.logger = logger
end

def start(config)
  source_url, topic = config.fetch_values :source_url, :topic
  retrieve_events(source_url) { |event| produce_event event, topic }
end

config = config_from_env
init_producer config
start config
