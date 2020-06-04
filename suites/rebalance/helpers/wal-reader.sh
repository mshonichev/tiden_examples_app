#!/bin/bash

BASE=/storage/ssd/mshonichev/tiden/rebalance-190408-131911/ignite/libs

CP="."
for i in $BASE/*.jar; do
    CP="$CP:$i"
done
for i in $BASE/ignite-indexing/*.jar; do
    CP="$CP:$i"
done
for i in $BASE/ignite-spring/*.jar; do
    CP="$CP:$i"
done


TEST=/storage/hdd/mshonichev/8.8.1.t18-biggerwal/work
NODE=node_1_1

#        -recordContainsText "AAAA"                      \
#        -recordTypes TX_RECORD,DATA_RECORD              \
#        -walTimeFromMillis
#        -walTimeToMillis

export IGNITE_HOME=$TEST/output
mkdir -p $IGNITE_HOME

export PATH=$JAVA_HOME/bin:$PATH

java \
    -cp "ignite-indexing-2.8.1.t16-tests.jar:$CP"               \
        org.apache.ignite.util.WalReader                        \
        -binaryMetadataFileStoreDir $TEST/$NODE/binary_meta     \
        -keepBinary                                             \
        -marshallerMappingFileStoreDir $TEST/$NODE/marshaller   \
        -pageSize 4096                                          \
        -walArchiveDir $TEST/$NODE/db/wal/archive/$NODE         \
        -recordTypes DATA_RECORD                                \
        -walDir $TEST/$NODE/db/wal/$NODE                        \

