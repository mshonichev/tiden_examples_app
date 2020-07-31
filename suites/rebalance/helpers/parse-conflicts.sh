#!/bin/bash

cat idle-verify.txt | sed 's/PartitionHashRecord/\nPartitionHashRecord/g' >conflicts.txt

cat conflicts.txt | grep -Eo "grpName=[^,]+" | cut -d'=' -f 2 | sort | uniq > conflict_groups.txt

grep "Started rebalance routine" *.log | cut -d' ' -f 3- > rebalance.txt

while read grp; do echo "Group: $grp"; grep $grp rebalance.txt; done < conflict_groups.txt

cat conflicts.txt | grep cache_group_3_088_0