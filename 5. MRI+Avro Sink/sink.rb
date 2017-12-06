# frozen_string_literal: true

require 'avro_turf/messaging'
require 'kafka' # TODO: maybe use Racecar

REGISTRY_URL = 'http://docker.for.mac.localhost:8081/'
TOPIC = 'article-change-events'
GROUP_ID = 'article-change-events-sink'
KAFKA_BROKERS = ['docker.for.mac.localhost:9092'].freeze
CONSUME_FROM_BEGINNING = false

# Shouldn't really be a constant but this is just a demo...
AVRO = AvroTurf::Messaging.new registry_url: REGISTRY_URL

def config
  { topic: TOPIC,
    group_id: GROUP_ID,
    brokers: KAFKA_BROKERS,
    consume_from_beginning: CONSUME_FROM_BEGINNING }
end

def create_subscribed_consumer(config)
  brokers, group_id, topic, consume_from_beginning =
    config.fetch_values(:brokers, :group_id, :topic, :consume_from_beginning)

  kafka = Kafka.new seed_brokers: brokers
  consumer = kafka.consumer group_id: group_id
  consumer.subscribe topic, start_from_beginning: consume_from_beginning
  trap('TERM') { consumer.stop }
  consumer
end

def start(config)
  consumer = create_subscribed_consumer config

  consumer.each_message do |event_record|
    print "#{event_record.offset}: "
    puts AVRO.decode event_record.value
  end

  nil
end

start config
