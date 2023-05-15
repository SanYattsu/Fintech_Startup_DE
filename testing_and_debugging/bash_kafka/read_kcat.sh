#!/bin/bash

kafkacat -C \
    -b rc1a-083hhcof7uqe71hd.mdb.yandexcloud.net:9091 \
    -G sdsdw transaction-service-input \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanisms=SCRAM-SHA-512 \
    -X sasl.username=producer_consumer \
    -X sasl.password="" \
    -X ssl.ca.location=/mnt/c/ca/YandexInternalRootCA.crt \
    -o beginning -e
    # -o end
    # -o -5
