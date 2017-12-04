# frozen_string_literal: true

require 'delivery_boy'
require 'logger'
require 'typhoeus'

URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
TOPIC = 'recent-change-events'
CLIENT_ID = 'recent-change-events-source'
KAFKA_BROKERS = ['docker.for.mac.localhost:9092'].freeze

def config_from_env
  { url: URL,
    topic: TOPIC,
    client_id: CLIENT_ID,
    brokers: KAFKA_BROKERS }
end

def retrieve_events(url)
  req = Typhoeus::Request.new url

  req.on_headers { |response| raise 'Request failed' if response.code != 200 }

  req.on_body do |chunk|
    chunk.each_line do |line|
      next unless line.start_with? 'data'
      data = line[6..-1]
      yield data
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
  url, topic = config.fetch_values :url, :topic
  retrieve_events(url) { |event| produce_event event, topic }
end

config = config_from_env
init_producer config
start config
