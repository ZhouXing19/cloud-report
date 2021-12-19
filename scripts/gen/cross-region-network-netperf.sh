#!/bin/bash

set -ex
pidfile="$HOME/cross-region-netperf-bench.pid"
f_force=''
f_wait=''
f_server=''
f_port=''
f_duration_latency=60
f_duration_throughput=500
f_server_mode=''

function usage() {
  echo "$1
Usage: $0 [-f] [-w] [-s server] -p port
  -s server: connect to netserver running on specified server.
  -p port: port number for netserver process
  -t <num>: throughput benchmark duration in seconds
  -l <num>: latency benchmark duration in seconds
  -f: ignore existing pid file; override and rerun.
  -w: wait for currently running benchmark to complete.
  -S: start netserver
"
  exit 1
}

while getopts 'fws:p:t:l:S' flag; do
  case "${flag}" in
    s) f_server="${OPTARG}" ;;
    p) f_port="${OPTARG}" ;;
    t) f_duration_throughput="${OPTARG}" ;;
    l) f_duration_latency="${OPTARG}" ;;
    f) f_force='true' ;;
    w) f_wait='true' ;;
    S) f_server_mode='true' ;;
    *) usage "";;
  esac
done

logdir="$HOME/cross-region-netperf-results"




if [ -n "$f_wait" ];
then
   exec sh -c "
    ( test -f '$logdir/success' ||
      (tail --pid \$(cat $pidfile) -f /dev/null && test -f '$logdir/success')
    ) || (echo 'Network benchmark did not complete successfully.  Check logs'; exit 1)"
fi

if [ -z "$f_port" ]
then
  usage "-p argument required"
fi

if [ -n "$f_server_mode" ];
then
   exec sh -c "sudo lsof -i :$f_port >/dev/null || sudo netserver -p $f_port"
fi

if [ -f "$pidfile" ] && [ -z "$f_force" ]
then
  pid=$(cat "$pidfile")
  echo "Netperf benchmark already running (pid $pid)"
  exit 1
fi

trap "rm -f $pidfile" EXIT SIGINT
echo $$ > "$pidfile"


if [ -z "$f_server" ]
then
  usage "server and port args required"
fi

rm -rf "$logdir"
mkdir "$logdir"
report="${logdir}/cross-region-netperf-results.log"

if [ -f $report ]
then
  rm $report
  fi


# TODO: run clients on multiple nodes.
(
  echo "Using $(netperf -V)"
  # Latency
  sudo netperf -H "$f_server" -p "$f_port" -l "$f_duration_latency" -I 99,5  -t TCP_RR -- -O min_latency,mean_latency,P90_LATENCY,P99_LATENCY,max_latency,stddev_latency,transaction_rate
  # Throughput
  cd newnetperf/netperf/doc/examples && ./multistream_netperf.sh -p "$f_port" -t "$f_duration_throughput"
  ) | tee "$report"



touch "$logdir/success"
