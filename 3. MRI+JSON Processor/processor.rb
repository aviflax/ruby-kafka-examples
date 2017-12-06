# frozen_string_literal: true

require 'json'
require 'kafka'
require 'rocksdb'

TOPIC_IN = 'article-change-events'
TOPIC_OUT = 'article-change-counts'
GROUP_ID = 'article-change-events-processor'
KAFKA_BROKERS = ['docker.for.mac.localhost:9092'].freeze
CONSUME_FROM_BEGINNING = true

# Shouldn't really be a constant but this _is_ just a demo...
ROCKSDB = RocksDB::DB.new '/tmp/article-change-counts.rocksdb'

def config
  { topic_in: TOPIC_IN,
    topic_out: TOPIC_OUT,
    group_id: GROUP_ID,
    brokers: KAFKA_BROKERS,
    consume_from_beginning: CONSUME_FROM_BEGINNING }
end

def create_subscribed_consumer(config, kafka)
  group_id, topic_in, consume_from_beginning =
    config.fetch_values(:group_id, :topic_in, :consume_from_beginning)

  consumer = kafka.consumer group_id: group_id
  consumer.subscribe topic_in, start_from_beginning: consume_from_beginning
  trap('TERM') { consumer.stop }
  consumer
end

def increment_article_change_count(article_title)
  key = article_title.to_s
  old_value = ROCKSDB.get(key) || '0'
  new_value = old_value.to_i + 1
  ROCKSDB.put key, new_value.to_s
  new_value
end

def article_title(change_event)
  change_event.fetch :title
end

def eligible?(change_event)
  article_title(change_event).is_a?(String) && !article_title(change_event).empty?
end

# Accepts a hash representing an article change event and returns the a new hash
# recording the total edit count for that article.
def transform(change_event)
  title = article_title change_event
  new_count = increment_article_change_count title
  { article_title: title,
    edit_count: new_count }
end

def process(event_record, config, kafka)
  change_event = JSON.parse event_record.value.to_s, symbolize_names: true

  return unless eligible? change_event

  key = article_title change_event
  value = transform change_event
  value_json = JSON.dump value

  kafka.deliver_message value_json, key: key, topic: config.fetch(:topic_out)
rescue StandardError => err
  # Log and then skip (drop) errors.
  puts "ERROR: #{err}", "VALUE: #{event_record.value}"
end

def start(config)
  kafka = Kafka.new seed_brokers: config.fetch(:brokers)
  consumer = create_subscribed_consumer config, kafka
  consumer.each_message { |event_record| process event_record, config, kafka }
  nil
end

start config
