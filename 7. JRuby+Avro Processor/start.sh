#!/bin/bash

TAG="jruby-avro-processor"

docker build -t $TAG . && docker run -it --rm $TAG
