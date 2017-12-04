#!/bin/bash

# can add --from-beginning if so desired

kafka-console-consumer --topic article-change-events \
                       --new-consumer --bootstrap-server localhost:9092 \
                       --property print.key=true \
                       --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
