FROM openjdk:8u212-jre-alpine

ARG gridgain_version={{gridgain_version}}
ARG kafka_version={{kafka_version}}
ARG scala_version=2.12

ENV GRIDGAIN_VERSION=$gridgain_version \
    KAFKA_VERSION=$kafka_version \
    SCALA_VERSION=$scala_version \
    IGNITE_HOME=/opt/gridgain \
    KAFKA_HOME=/opt/kafka

ENV PATH=${PATH}:${KAFKA_HOME}/bin

RUN apk add --no-cache bash curl docker \
 && wget http://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -O "/tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
 && tar -C /tmp/ -xzf /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
 && mv /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka \
 && rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
 && wget "https://gridgain.com/media/gridgain-ultimate-${GRIDGAIN_VERSION}.zip" -O "/tmp/gridgain-ultimate-${GRIDGAIN_VERSION}.zip" \
 && unzip /tmp/gridgain-ultimate-${GRIDGAIN_VERSION}.zip -d /tmp \
 && mv /tmp/gridgain-ultimate-${GRIDGAIN_VERSION} /opt/gridgain \
 && rm /tmp/gridgain-ultimate-${GRIDGAIN_VERSION}.zip \
 && $IGNITE_HOME/integration/gridgain-kafka-connect/copy-dependencies.sh \
 && mkdir -p /opt/kafka/connect/ \
 && cp -r $IGNITE_HOME/integration/gridgain-kafka-connect /opt/kafka/connect/ \
 && echo "key.converter=org.apache.kafka.connect.json.JsonConverter" > /opt/kafka/config/connect-standalone.properties \
 && echo "value.converter=org.apache.kafka.connect.json.JsonConverter" >> /opt/kafka/config/connect-standalone.properties \
 && echo "key.converter.schemas.enable=true" >> /opt/kafka/config/connect-standalone.properties \
 && echo "value.converter.schemas.enable=true" >> /opt/kafka/config/connect-standalone.properties \
 && echo "offset.storage.file.filename=/tmp/connect.offsets" >> /opt/kafka/config/connect-standalone.properties \
 && echo "offset.flush.interval.ms=10000" >> /opt/kafka/config/connect-standalone.properties \
 && echo "plugin.path=/opt/kafka/connect/gridgain-kafka-connect" >> /opt/kafka/config/connect-standalone.properties \
 && echo "bootstrap.servers={{KAFKA_CONNECT_BOOTSTRAP_SERVERS}}" >> /opt/kafka/config/connect-standalone.properties

COPY sink.properties sink-connector.xml source.properties source-connector.xml /opt/kafka/connect/

COPY start-kafka-connector.sh /usr/local/bin/

RUN chmod +x /usr/local/bin/start-kafka-connector.sh

# Use "exec" form so that it runs as PID 1 (useful for graceful shutdown)
CMD ["/usr/local/bin/start-kafka-connector.sh"]
