#!/bin/bash

TAG="mri-json-sink"

docker build -t $TAG . && docker run -it --rm $TAG
