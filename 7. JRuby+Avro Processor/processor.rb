# frozen_string_literal: true

require 'avro_turf/messaging'

require 'java' # magic! see https://github.com/jruby/jruby/wiki/CallingJavaFromJRuby

Dir.glob('jar/*.jar') { |path| puts "requiring #{path}"; require_relative path }

java_import org.apache.kafka.common.serialization.Serde
java_import org.apache.kafka.common.serialization.Serdes
java_import org.apache.kafka.streams.KafkaStreams
java_import org.apache.kafka.streams.StreamsBuilder
java_import org.apache.kafka.streams.StreamsConfig
java_import org.apache.kafka.streams.kstream.KStream
java_import org.apache.kafka.streams.kstream.KTable
java_import org.apache.kafka.streams.kstream.Produced

# Fake environment variables!
TOPIC_IN = 'article-change-events'
TOPIC_OUT = 'article-change-counts'
REGISTRY_URL = 'http://docker.for.mac.localhost:8081/'
SCHEMATA_PATH = './'
APPLICATION_ID = 'article-change-events-count-processor'
KAFKA_BROKERS = 'docker.for.mac.localhost:9092'
CONSUME_FROM_BEGINNING = true

# Shouldn't really be a constant but this _is_ just a demo...
AVRO = AvroTurf::Messaging.new registry_url: REGISTRY_URL,
                               schemas_path: SCHEMATA_PATH

def config_from_env
  { topic_in: TOPIC_IN,
    topic_out: TOPIC_OUT,
    application_id: APPLICATION_ID,
    brokers: KAFKA_BROKERS,
    consume_from_beginning: CONSUME_FROM_BEGINNING }
end

def to_streams_config(config)
  serdes_class_name = Serdes.String.get_class.get_name

  StreamsConfig.new(
    StreamsConfig::APPLICATION_ID_CONFIG => config.fetch(:application_id),
    StreamsConfig::CLIENT_ID_CONFIG => config.fetch(:application_id),
    StreamsConfig::BOOTSTRAP_SERVERS_CONFIG => config.fetch(:brokers),
    StreamsConfig::DEFAULT_KEY_SERDE_CLASS_CONFIG => serdes_class_name,
    StreamsConfig::DEFAULT_VALUE_SERDE_CLASS_CONFIG => serdes_class_name,

    # Records should be flushed every 10 seconds. This is less than the default
    # in order to keep this example interactive.
    StreamsConfig::COMMIT_INTERVAL_MS_CONFIG => 10 * 1000,

    # For illustrative purposes we disable record caches.
    StreamsConfig::CACHE_MAX_BYTES_BUFFERING_CONFIG => 0
  )
end

def build_topology(config)
  topic_in, topic_out = config.fetch_values :topic_in, :topic_out

  builder = StreamsBuilder.new

  builder
    .stream(topic_in)
    .map_values(->(event) { AVRO.decode(event) })
    .group_by_key # TODO: change to group by article and day, I think
    .count
    .to_stream
    .to(topic_out, Produced.with(Serdes.Long, Serdes.Long))

  builder.build
end

def start(config)
  topology = build_topology config
  streams_config = to_streams_config config
  streams = KafkaStreams.new topology, streams_config
  streams.start
end

start config_from_env
