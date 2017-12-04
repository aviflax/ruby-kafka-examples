#!/bin/bash

TAG="mri-avro-source"

docker build -t $TAG . && docker run -it --rm $TAG

