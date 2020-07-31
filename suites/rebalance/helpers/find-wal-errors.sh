#!/bin/bash

cacheId="-2068581311"

logs=(node_1_1_data_record.log node_1_2_data_record.log node_1_3_data_record.log)

echo "Processing ..."
#if false; then

for logfile in ${logs[@]}; do
    echo "Preprocess file $logfile"
    rm -f $logfile.$cacheId.log 2>/dev/null
    grep "cacheId=$cacheId" $logfile | while read ll; do
        timestamp=$(echo $ll | grep -Eo "timestamp=[0-9]+")
        echo $ll | sed "s/$timestamp//" | sed "s/UnwrapDataEntry/\n$timestamp : UnwrapDataEntry/g" >> $logfile.$cacheId.log
    done
done

#fi

rm -f wal-errors.$cacheId.*.* 2>/dev/null
for k in `seq 0 100000`; do
    echo ">>> key = $k"
    for logfile in ${logs[@]}; do
        nodeId=$(echo $logfile | grep -Eo "node_[0-9\_]+" | rev | cut -c 2- | rev)
        opsFile="cache.$cacheId/node.$nodeId/keys/$logfile.$cacheId.$k.log"
        echo " node = $nodeId"
        mkdir -p "cache.$cacheId/node.$nodeId/keys"
        grep "k = $k," $logfile.$cacheId.log | grep -Eo "(op=(DELETE|CREATE|UPDATE))|timestamp=[0-9]+" | sed 's/timestamp=/!/' | sed 's/op=/ /' | tr -d '\n' | tr '!' '\n' | sort -k 1 -n | tail -n +2 | tee $opsFile
        partId=$(grep "k = $k," $logfile.$cacheId.log | head -n 1 | grep -Eo "partId=[0-9]+")

        if [ "$nodeId" = "node_1_1" ]; then
            numOps=$(cat $opsFile | wc -l)
            firstOp=$(head $opsFile -n 1)
            primaryOps=$opsFile
            if [ "$numOps" = "0" ]; then
                rm -f $opsFile
                break;
            fi
        fi
        if [ "$(echo $firstOp | grep -Eo 'DELETE' 2>/dev/null)" = "DELETE" ]; then
            echo "ERROR: Cache $cacheId, Partition $partId, Key $k, Node $nodeId :  DELETE goes before CREATE" | tee -a wal-errors.$cacheId.1.log
        fi
        if [ ! "$nodeId" = "node_1_1" ]; then
            numBackupOps=$(cat $opsFile | wc -l)
            if [ ! "$numBackupOps" = "$numOps" ]; then
                echo "ERROR: Cache $cacheId, Partition $partId, Key $k, Node $nodeId total operations differs from primary node" | tee -a wal-errors.$cacheId.2.log
            else
                cat $primaryOps | awk '{print $2} ' > /tmp/f$cacheId.1
                cat $opsFile | awk '{print $2} ' > /tmp/f$cacheId.2
                if ! diff -Naurw /tmp/f$cacheId.1 /tmp/f$cacheId.2 >/dev/null 2>&1; then
                    echo "ERROR: Cache $cacheId, Partition $partId, Key $k, Node $nodeId operations different from primary node" | tee -a wal-errors.$cacheId.3.log | tee -a wal-errors.$cacheId.3.diff
                    diff -Naurw $primaryOps $opsFile | tee -a wal-errors.$cacheId.3.diff
                fi
            fi
        fi
    done
#    if [ $k -gt 1 ]; then
#        break
#    fi
done



