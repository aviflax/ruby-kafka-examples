#!/bin/bash

# can add --from-beginning if so desired

kafka-avro-console-consumer --topic article-change-events \
                            --bootstrap-server localhost:9092 \
                            --property print.key=true \
                            --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
