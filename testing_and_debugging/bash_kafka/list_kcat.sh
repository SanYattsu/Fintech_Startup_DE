#!/bin/bash

docker run -it \
    -v "/mnt/c/ca/CA.pem:/data/CA.pem" \
    --network=host edenhill/kcat:1.7.1 \
    -b rc1a-083hhcof7uqe71hd.mdb.yandexcloud.net:9091 \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanisms=SCRAM-SHA-512 \
    -X sasl.username=producer_consumer \
    -X sasl.password="" \
    -X ssl.ca.location=/data/CA.pem \
    -L -J
