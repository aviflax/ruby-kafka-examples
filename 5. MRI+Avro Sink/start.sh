#!/bin/bash

TAG="mri-avro-sink"

docker build -t $TAG . && docker run -it --rm $TAG
