#!/bin/bash

LOG_DIR="."
TMP_DIR="."
LOG_FMT="grid.srv.*.log"
NOTLOG_FMT="activate|deactivate"

tmp_log=$TMP_DIR/exchange_after_leave.tmp.log

# find exchange on server node re-join
echo "Checking for re-joined server node"


node_joined_message=$(grep "Added new node" $tmp_log | grep "isClient=false" | head -n 1)
#echo $node_joined_message
if [ "$node_joined_message" = "" ]; then
    echo "No joined server node message found"
    exit 0
else
    joined_node_id=$(echo $node_joined_message | grep -Eo "id=[0-9a-f\-]+" | cut -d '=' -f 2)
    echo "Found: $joined_node_id"
fi

echo "Looking for started exchange on node join"
started_exchange_message=$(grep "Started exchange" $tmp_log | grep "NODE_JOINED" | grep $joined_node_id)
if [ "$started_exchange_message" = "" ]; then
    echo "ERROR: can't find exchange for node $joined_node_id leave"
    exit 1
else
    exchange_topVer=$(echo $started_exchange_message | grep -Eo "AffinityTopologyVersion \[topVer=[0-9]+, minorTopVer=[0-9]+\]")
    echo "Found, exchange version: $exchange_topVer"
fi

echo "Looking for finish exchange future"
finished_exchange_message=$(grep "Finish exchange future" $tmp_log | grep "startVer=${exchange_topVer/\[/\\[}")
finished_exchange_message2=$(grep -A 5 "Finish exchange future" $tmp_log | grep -A 5 "startVer=${exchange_topVer/\[/\\[}" | tail -n 1)

if [ "$finished_exchange_message" = "" ]; then
    echo "ERROR: can't find finish exchange future, check logs"
    exit 1
fi

started_exchange_time=$(echo $started_exchange_message | grep -Eo "^\[[0-9\:]+")
finished_exchange_time=$(echo $finished_exchange_message | grep -Eo "^\[[0-9\:]+")
finished_exchange_time2=$(echo $finished_exchange_message2 | grep -Eo "^\[[0-9\:]+")

echo "Found, exchange duration: $started_exchange_time .. $finished_exchange_time"

echo "Splitting exchange log"
cat $tmp_log \
    | grep -A 1000000000 "${started_exchange_time/\[/\\[}" \
    | tee $TMP_DIR/exchange_after_join.tmp.log \
    | grep -B 1000000000 "${finished_exchange_time2/\[/\\[}" \
    > $TMP_DIR/exchange_messages_during_join.tmp.log

echo "Dumping exchange stages"
cat $TMP_DIR/exchange_messages_during_join.tmp.log | grep -E "(applied|performed)"


exchange2_topVer=${exchange_topVer/minorTopVer=0/minorTopVer=1}

echo "Looking for started exchange #2 on node join"
started_exchange2_message=$(grep "Started exchange" $tmp_log | grep "${exchange2_topVer/\[/\\[}")
if [ "$started_exchange2_message" = "" ]; then
    echo "ERROR: can't find exchange #2 for node $joined_node_id leave"
    exit 1
else
#    exchange_topVer=$(echo $started_exchange_message | grep -Eo "AffinityTopologyVersion \[topVer=[0-9]+, minorTopVer=[0-9]+\]")
    echo "Found, exchange version: $exchange2_topVer"
fi

echo "Looking for finish exchange #2 future"
finished_exchange2_message=$(grep "Finish exchange future" $tmp_log | grep "startVer=${exchange2_topVer/\[/\\[}")
finished_exchange2_message2=$(grep -A 5 "Finish exchange future" $tmp_log | grep -A 5 "startVer=${exchange2_topVer/\[/\\[}" | tail -n 1)

if [ "$finished_exchange_message" = "" ]; then
    echo "ERROR: can't find finish exchange future, check logs"
    exit 1
fi

started_exchange2_time=$(echo $started_exchange2_message | grep -Eo "^\[[0-9\:]+")
finished_exchange2_time=$(echo $finished_exchange2_message | grep -Eo "^\[[0-9\:]+")
finished_exchange2_time2=$(echo $finished_exchange2_message2 | grep -Eo "^\[[0-9\:]+")

echo "Found, exchange2 duration: $started_exchange2_time .. $finished_exchange2_time2"

echo "Splitting exchange2 log"
cat $tmp_log \
    | grep -A 1000000000 "${started_exchange2_time/\[/\\[}" \
    | tee $TMP_DIR/exchange_after_join_2.tmp.log \
    | grep -B 1000000000 "${finished_exchange2_time2/\[/\\[}" \
    > $TMP_DIR/exchange_messages_during_join_2.tmp.log

echo "Dumping exchange stages"
cat $TMP_DIR/exchange_messages_during_join.tmp.log | grep -E "(applied|performed)"



