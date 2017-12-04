#!/bin/bash

docker build -t app .
docker run -it --rm app
