# frozen_string_literal: true

require 'delivery_boy'
require 'logger'
require 'typhoeus'

URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
TOPIC = 'recent-change-events'
CLIENT_ID = 'recent-change-events-source'
KAFKA_BROKERS = ['docker.for.mac.localhost:9092'].freeze

def retrieve_events
  req = Typhoeus::Request.new URL

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

def produce_event(event)
  DeliveryBoy.deliver event, topic: TOPIC
end

def config
  DeliveryBoy.configure do |config|
    config.client_id = CLIENT_ID
    config.brokers = KAFKA_BROKERS
  end

  logger = Logger.new $stdout
  logger.level = Logger::INFO
  DeliveryBoy.logger = logger
end

def start
  retrieve_events { |event| produce_event event }
end

config
start
