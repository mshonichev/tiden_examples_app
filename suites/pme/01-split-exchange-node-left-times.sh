#!/bin/bash

#RUN="pme-181108-164906-1serv-p150-32k"
#RUN="pme-181113-134011-p150-1serv-28k"
#RUN="pme-181113-131634-p150-1serv-24k"
#RUN="pme-181113-141241-p150-1serv-20k"
#RUN="pme-181112-171525-p150-1serv-16k"
#RUN="pme-181112-165734-p150-1serv-12k"
#RUN="pme-181112-162911-p150-1serv-8k"
#RUN="pme-181112-161329-p150-1serv-4k"

RUN="pme-181114-151644-p160-1serv-32k"

#RUN="pme-181114-170440"
#RUN="pme-181114-160943-p150-1serv-32k"

LOG_DIR=$(dirname $0)"/../../var/$RUN/test_pme_bench/TestPmeBench/test_pme_bench_1_server/"
TMP_DIR="$LOG_DIR/tmp"
LOG_FMT="*_logs/grid.srv.*.log"
NOTLOG_FMT="activate|deactivate"

mkdir -p $TMP_DIR

crd_log=
crd_node_id=

# find coordinator log
echo "Searching coordinator node"
for log in $LOG_DIR/$LOG_FMT; do
    if [ "$(echo $log | grep -oE "$NOTLOG_FMT")" = "" ]; then
        local_node_id_message=$(grep "Local node \[ID=" $log)
        node_order=$(echo $local_node_id_message | grep -Eo "order=[0-9]+," | cut -d '=' -f 2 | tr -d ',')
        if [ "$node_order" = "1" ]; then
            crd_log=$log
            crd_node_id=$(echo $local_node_id_message | grep -Eo "\[ID=[0-9A-F\-]+" | cut -d'=' -f 2)
            break
        fi
    fi
done

if [ "$crd_log" = "" ]; then
    echo "ERROR: cant' find coordinator node log"
    exit 1
fi
echo "Found coordinator: $crd_node_id"


# find exchange on server node leave
echo "Checking for failed server node"

node_failed_message=$(grep "Node FAILED" $crd_log | grep "isClient=false" | head -n 1)

if [ "$node_failed_message" = "" ]; then
    echo "No failed server nodes found"
    exit 0
else
    failed_node_id=$(echo $node_failed_message | grep -Eo "id=[0-9a-f\-]+" | cut -d '=' -f 2)
    echo "Found: $failed_node_id"
fi


for log in $LOG_DIR/$LOG_FMT; do
    if [ "$(echo $log | grep -oE "$NOTLOG_FMT")" = "" ]; then

        local_node_id_message=$(grep "Local node \[ID=" $log)
        node_order=$(echo $local_node_id_message | grep -Eo "order=[0-9]+," | cut -d '=' -f 2 | tr -d ',')
        node_id=$(echo $local_node_id_message | grep -Eo "\[ID=[0-9A-F\-]+" | cut -d'=' -f 2)
        node_log_suffix=".$node_order.$node_id"

        echo "---[ Analyse log $log ]---"

        echo "Looking for started exchange on node failure"
        started_exchange_message=$(grep "Started exchange" $log | grep "NODE_FAILED" | grep $failed_node_id)

        if [ "$started_exchange_message" = "" ]; then
            echo "WARN: can't find exchange for node $failed_node_id leave, skip"
            continue
        else
            exchange_topVer=$(echo $started_exchange_message | grep -Eo "AffinityTopologyVersion \[topVer=[0-9]+, minorTopVer=[0-9]+\]")
            echo "Found, exchange version: $exchange_topVer"
        fi

        echo "Looking for finish exchange future"
        finished_exchange_message=$(grep "Finish exchange future" $log | grep "startVer=${exchange_topVer/\[/\\[}")
        finished_exchange_message2=$(grep -A 5 "Finish exchange future" $log | grep -A 5 "startVer=${exchange_topVer/\[/\\[}" | tail -n 1)

        if [ "$finished_exchange_message" = "" ]; then
            echo "ERROR: can't find finish exchange future, skip"
            continue
        fi

        started_exchange_time=$(echo $started_exchange_message | grep -Eo "^\[[0-9\:]+")
        finished_exchange_time=$(echo $finished_exchange_message | grep -Eo "^\[[0-9\:]+")
        finished_exchange_time2=$(echo $finished_exchange_message2 | grep -Eo "^\[[0-9\:]+")

        echo "Found, exchange duration: $started_exchange_time .. $finished_exchange_time"

        echo "Splitting exchange log"
        cat $log \
            | grep -A 10000000 "${started_exchange_time/\[/\\[}" \
            | tee $TMP_DIR/exchange_after_leave.$node_log_suffix.tmp.log \
            | grep -B 10000000 "${finished_exchange_time2/\[/\\[}" \
            > $TMP_DIR/exchange_messages_during_leave.$node_log_suffix.tmp.log

        echo "Dumping exchange stages"
        cat $TMP_DIR/exchange_messages_during_leave.$node_log_suffix.tmp.log | grep -E "(applied|performed|Finished restoring|JVM pause)"
    fi
done
