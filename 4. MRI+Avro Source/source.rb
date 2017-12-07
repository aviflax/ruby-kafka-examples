# frozen_string_literal: true

require 'avro_turf/messaging'
require 'delivery_boy'
require 'json'
require 'logger'
require 'typhoeus'

SOURCE_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
REGISTRY_URL = 'http://docker.for.mac.localhost:8081/'
SCHEMATA_PATH = './'
TOPIC = 'article-change-events'
CLIENT_ID = 'article-change-events-source'
KAFKA_BROKERS = ['docker.for.mac.localhost:9092'].freeze

# Shouldn't really be a constant but this _is_ just a demo...
AVRO = AvroTurf::Messaging.new registry_url: REGISTRY_URL,
                               schemas_path: SCHEMATA_PATH

def config_from_env
  { source_url: SOURCE_URL,
    registry_url: REGISTRY_URL,
    topic: TOPIC,
    client_id: CLIENT_ID,
    brokers: KAFKA_BROKERS }
end

def to_data(raw_line)
  return nil unless raw_line.start_with? 'data:'

  # Remove whitespace; it can cause problems.
  stripped_line = raw_line.strip

  # Skip objects that are split across multiple chunks.
  return nil unless stripped_line.end_with? '}'

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

def article_title(event)
  event.fetch(:title)
       .force_encoding('utf-8')
end

def to_avro(event)
  smaller_event = {
    'article_title' => article_title(event),
    'timestamp' => event.fetch(:timestamp)
  }

  AVRO.encode smaller_event, schema_name: 'article_change_event'
end

def produce!(event, topic)
  DeliveryBoy.deliver to_avro(event),
                      key: article_title(event),
                      topic: topic
  nil
rescue StandardError => err
  puts "ERROR: #{err}"
  err.backtrace.each { |line| puts "\t#{line}"}
  nil
end

def init_producer!(config)
  DeliveryBoy.configure do |db_config|
    db_config.client_id = config.fetch :client_id
    db_config.brokers = config.fetch :brokers
  end

  logger = Logger.new $stdout
  logger.level = Logger::INFO
  DeliveryBoy.logger = logger

  nil
end

def start(config)
  init_producer! config

  source_url, topic = config.fetch_values :source_url, :topic

  retrieve_events(source_url) do |raw_event|
    event = JSON.parse raw_event, symbolize_names: true
    produce! event, topic
  end
end

start config_from_env
