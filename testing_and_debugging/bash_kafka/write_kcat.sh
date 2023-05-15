#!/bin/bash

kafkacat -P \
    -b rc1a-083hhcof7uqe71hd.mdb.yandexcloud.net:9091 \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanisms=SCRAM-SHA-512 \
    -X sasl.username=producer_consumer \
    -X sasl.password="" \
    -X ssl.ca.location=/mnt/c/ca/YandexInternalRootCA.crt \
    -t transaction-service-input \
    -K:

# my_key:my_message -- формат отправки сообщения >> ctrl+D
