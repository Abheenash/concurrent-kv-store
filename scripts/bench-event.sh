#!/usr/bin/env bash
# kqueue/epoll vs poll, same workloads. Appends to results/bench-event-<os>.csv
set -e
cd "$(dirname "$0")/.."
B=./build
run() { local label=$1; shift; $B/kvserver --port 5599 "$@" >/dev/null 2>&1 & local pid=$!; sleep 0.4
  $B/kvbench --port 5599 --csv --label "$label" $BENCH; kill -INT $pid; wait $pid 2>/dev/null || true; }
echo "label,clients,pipeline,get_ratio,requests,seconds,req_per_s,p50_us,p90_us,p99_us,p999_us,errors"
for m in poll event; do
  BENCH="--clients 50 --requests 1000000 --pipeline 1";  run "$m x10 / 50 clients"              --mode $m
  BENCH="--clients 50 --requests 4000000 --pipeline 32"; run "$m x10 / 50 clients / pipeline 32" --mode $m
  BENCH="--clients 500 --requests 1000000 --pipeline 1"; run "$m x10 / 500 clients"             --mode $m
  BENCH="--clients 2000 --requests 1000000 --pipeline 1"; run "$m x10 / 2000 clients"           --mode $m
done
