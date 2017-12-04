#!/bin/bash

TAG="mri-json-processor"

docker build -t $TAG . && docker run -it --rm $TAG
