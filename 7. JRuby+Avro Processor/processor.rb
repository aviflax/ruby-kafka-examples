# frozen_string_literal: true

require 'avro_turf/messaging'

require 'java' # magic! see https://github.com/jruby/jruby/wiki/CallingJavaFromJRuby

Dir.glob('jar/*.jar') { |path| require_relative path }

java_import org.apache.kafka.common.serialization.Serdes
java_import org.apache.kafka.streams.KafkaStreams
java_import org.apache.kafka.streams.StreamsBuilder
java_import org.apache.kafka.streams.StreamsConfig
java_import org.apache.kafka.streams.Consumed
java_import org.apache.kafka.streams.kstream.Produced

# Fake environment variables!
TOPIC_IN = 'article-change-events'
TOPIC_OUT = 'article-change-counts'
REGISTRY_URL = 'http://docker.for.mac.localhost:8081/'
APPLICATION_ID = 'article-change-events-count-processor'
KAFKA_BROKERS = 'docker.for.mac.localhost:9092'
CONSUME_FROM_BEGINNING = true

def config_from_env
  { topic_in: TOPIC_IN,
    topic_out: TOPIC_OUT,
    application_id: APPLICATION_ID,
    brokers: KAFKA_BROKERS,
    consume_from_beginning: CONSUME_FROM_BEGINNING }
end

def to_streams_config(config)
  StreamsConfig.new(
    StreamsConfig::APPLICATION_ID_CONFIG => config.fetch(:application_id),
    StreamsConfig::BOOTSTRAP_SERVERS_CONFIG => config.fetch(:brokers),
    StreamsConfig::DEFAULT_KEY_SERDE_CLASS_CONFIG => Serdes.String.get_class.get_name,
    StreamsConfig::DEFAULT_VALUE_SERDE_CLASS_CONFIG => Serdes.ByteArray.get_class.get_name,

    # Flush records every 5 seconds. This is less than the default
    # in order to keep this example interactive.
    StreamsConfig::COMMIT_INTERVAL_MS_CONFIG => 5 * 1000,

    # For illustrative purposes we disable record caches.
    StreamsConfig::CACHE_MAX_BYTES_BUFFERING_CONFIG => 0
  )
end

class AvroDeserializer
  def initialize
    @avro = AvroTurf::Messaging.new registry_url: REGISTRY_URL
  end

  def deserialize(_topic, data)
    # Convert the Java bytearray into a Ruby String because that’s what AvroTurf::Messaging#decode
    # expects; it can't/won't do an implicit conversion.
    data_string = data.to_s

    # Decode the Avro and then — since we know it’s a hash with string keys — symbolize the keys.
    @avro.decode(data_string)
         .map { |k, v| [k.to_sym, v] }
         .to_h
  end
end

class AvroSerde
  def deserializer
    AvroDeserializer.new
  end
end

def eligible?(event)
  event[:article_title].is_a?(String) && !event[:article_title].empty?
end

def build
  builder = StreamsBuilder.new
  yield builder
  builder.build
end

def build_topology(config)
  topic_in, topic_out = config.fetch_values :topic_in, :topic_out
  string_serdes = Serdes.String

  build do |builder|
    builder.stream(topic_in, Consumed.with(string_serdes, AvroSerde.new))
           .filter(->(_key, event) { eligible? event })
           .group_by_key
           .count
           .to_stream
           .to(topic_out, Produced.with(string_serdes, Serdes.Long))
  end
end

def start(config)
  topology = build_topology config
  streams_config = to_streams_config config
  streams = KafkaStreams.new topology, streams_config
  streams.start
end

start config_from_env
