#!/bin/bash

docker run -v /Users/weiaws/lab/typescript-test/builder-creed/backend/backend_functions/functions:/root/data python:3.9.9-slim-buster pip3 install -r /root/data/requirements.txt -t /root/data/lib