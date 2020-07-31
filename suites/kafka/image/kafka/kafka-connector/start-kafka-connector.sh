#!/bin/sh -e

sed -i -e 's/{{KAFKA_CONNECT_BOOTSTRAP_SERVERS}}/'$KAFKA_CONNECT_BOOTSTRAP_SERVERS'/g' /opt/kafka/config/connect-standalone.properties

sed -i -e 's/{{SOURCE_TASKS_MAX}}/'$SOURCE_TASKS_MAX'/g' /opt/kafka/connect/source.properties
sed -i -e 's/{{SOURCE_TOPIC_PREFIX}}/'$SOURCE_TOPIC_PREFIX'/g' /opt/kafka/connect/source.properties
sed -i -e 's/{{SOURCE_SHALL_LOAD_INITIAL_DATA}}/'$SOURCE_SHALL_LOAD_INITIAL_DATA'/g' /opt/kafka/connect/source.properties
sed -i -e 's/{{FAILOVER_POLICY}}/'$FAILOVER_POLICY'/g' /opt/kafka/connect/source.properties

sed -i -e 's/{{SOURCE_IP_FINDER_ADDRESSES}}/'$SOURCE_IP_FINDER_ADDRESSES'/g' /opt/kafka/connect/source-connector.xml

sed -i -e 's/{{SINK_TOPICS}}/'$SINK_TOPICS'/g' /opt/kafka/connect/sink.properties
sed -i -e 's/{{SINK_TASKS_MAX}}/'$SINK_TASKS_MAX'/g' /opt/kafka/connect/sink.properties
sed -i -e 's/{{SINK_TOPIC_PREFIX}}/'$SINK_TOPIC_PREFIX'/g' /opt/kafka/connect/sink.properties
sed -i -e 's/{{SINK_PUSH_INTERVAL}}/'$SINK_PUSH_INTERVAL'/g' /opt/kafka/connect/sink.properties

sed -i -e 's/{{SINK_IP_FINDER_ADDRESSES}}/'$SINK_IP_FINDER_ADDRESSES'/g' /opt/kafka/connect/sink-connector.xml

exec "$KAFKA_HOME/bin/connect-standalone.sh" "$KAFKA_HOME/config/connect-standalone.properties" "$KAFKA_HOME/connect/source.properties" "$KAFKA_HOME/connect/sink.properties"
