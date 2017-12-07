#!/bin/bash

# can add --from-beginning if so desired

kafka-console-consumer --topic article-change-counts \
                       --bootstrap-server localhost:9092 \
                       --property print.key=true \
                       --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
                       --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
