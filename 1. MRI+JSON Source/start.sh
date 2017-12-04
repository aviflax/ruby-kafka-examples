#!/bin/bash

TAG="mri-json-source"

docker build -t $TAG . && docker run -it --rm $TAG

