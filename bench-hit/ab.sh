#!/bin/sh
# Generic macOS A/B. Usage: ab.sh PAIRS "WORKLOADS" "ENTRIES" "WORKERS" OUT VARIANT...
# Each VARIANT is LABEL=BINARY:BACKEND. Order alternates per pair.
set -eu
pairs=$1; workloads=$2; entries_list=$3; workers_list=$4; out=$5; shift 5
variants="$*"
for pair in $(seq 1 "$pairs"); do
  for entries in $entries_list; do
    for workers in $workers_list; do
      for workload in $workloads; do
        case $workload in resident-hit|read-miss) ops=${READ_OPS:-16000000};; *) ops=${WRITE_OPS:-4000000};; esac
        list=$variants; [ $((pair % 2)) -eq 0 ] && list=$(echo $variants | tr ' ' '\n' | sort -r | tr '\n' ' ')
        for v in $list; do
          label=${v%%=*}; rest=${v#*=}; bin=${rest%%:*}; backend=${rest#*:}
          printf '%s,%s,' "$label" "$entries" >> $out
          $bin $backend "$workload" "$entries" "$ops" "$workers" 64 "$pair" 0 | tail -n 1 >> $out
        done
      done
    done
  done
done
