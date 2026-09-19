#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."
B=./build
run() { # label, server args..., then bench args via BENCH env
  local label=$1; shift
  $B/kvserver --port 5599 "$@" >/dev/null 2>&1 & local pid=$!
  sleep 0.4
  $B/kvbench --port 5599 --csv --label "$label" $BENCH
  kill -INT $pid; wait $pid 2>/dev/null || true
}
echo "label,clients,pipeline,get_ratio,requests,seconds,req_per_s,p50_us,p90_us,p99_us,p999_us,errors"
BENCH="--clients 50 --requests 1000000 --pipeline 1"
run "thread-per-conn / 64 shards"   --mode thread --shards 64
run "poll x10 / 64 shards"          --mode poll --shards 64
run "poll x10 / 1 shard (global lock)" --mode poll --shards 1
run "thread-per-conn / 1 shard"     --mode thread --shards 1
BENCH="--clients 50 --requests 4000000 --pipeline 32"
run "poll x10 / 64 shards / pipeline 32" --mode poll --shards 64
run "poll x10 / 1 shard / pipeline 32"   --mode poll --shards 1
run "thread-per-conn / 64 shards / pipeline 32" --mode thread --shards 64
BENCH="--clients 500 --requests 1000000 --pipeline 1"
run "poll x10 / 500 clients"        --mode poll --shards 64
run "thread-per-conn / 500 clients" --mode thread --shards 64
BENCH="--clients 50 --requests 500000 --pipeline 1 --get-ratio 0.5"
run "poll / aof everysec / 50% writes" --mode poll --aof bench.aof --fsync everysec
run "poll / aof always / 50% writes"   --mode poll --aof bench2.aof --fsync always
run "poll / no aof / 50% writes"       --mode poll
rm -f bench.aof bench2.aof
