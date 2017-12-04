# frozen_string_literal: true

require 'avro_turf/messaging'
require 'delivery_boy'
require 'json'
require 'logger'
require 'typhoeus'

SOURCE_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
REGISTRY_URL = 'http://docker.for.mac.localhost:8081/'
SCHEMATA_PATH = './'
TOPIC = 'recent-change-events'
CLIENT_ID = 'recent-change-events-source'
KAFKA_BROKERS = ['docker.for.mac.localhost:9092'].freeze

# Shouldn't really be a constant but this is just a demo...
AVRO = AvroTurf::Messaging.new registry_url: REGISTRY_URL, schemas_path: SCHEMATA_PATH

def config_from_env
  { source_url: SOURCE_URL,
    registry_url: REGISTRY_URL,
    topic: TOPIC,
    client_id: CLIENT_ID,
    brokers: KAFKA_BROKERS }
end

def retrieve_events(source_url)
  req = Typhoeus::Request.new source_url

  req.on_headers { |response| raise 'Request failed' if response.code != 200 }

  req.on_body do |chunk|
    chunk.each_line do |raw_line|
      next unless raw_line.start_with? 'data'

      # Remove whitespace; it can cause problems.
      stripped_line = raw_line.strip

      # Skip objects that are split across multiple chunks.
      next unless stripped_line.end_with?('}')

      # Remove the prefix 'data: '
      data = stripped_line[6..-1]

      # Callback time!
      yield data
    end
  end

  req.run
end

def to_avro(event, registry_url)
  smaller_event = { 'id' => event.fetch(:id) }
  AVRO.encode smaller_event, schema_name: 'article_change_event'
end

def eligible?(event)
  event[:id].is_a? Integer
end

def produce_event(event, topic, registry_url)
  event_avro = to_avro event, registry_url
  DeliveryBoy.deliver event_avro, topic: topic
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
  source_url, registry_url, topic = config.fetch_values :source_url, :registry_url, :topic
  retrieve_events(source_url) do |raw_event|
    event = JSON.parse raw_event, symbolize_names: true
    return unless eligible? event
    produce_event event, topic, registry_url
  end
end

config = config_from_env
init_producer config
start config