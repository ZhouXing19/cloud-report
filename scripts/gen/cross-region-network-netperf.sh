#!/bin/bash

set -ex
pidfile="$HOME/cross-region-netperf-bench.pid"
f_force=''
f_wait=''
k_wait=''
f_server=''
f_port=''
f_duration_latency=60
f_duration_throughput=240
f_server_mode=''
draw_plot_duration=0
test_mode='cross-region'

machine_name="unknown machine"

function usage() {
  echo "$1
Usage: $0 [-f] [-w] [-s server] -p port
  -s server: connect to netserver running on specified server.
  -p port: port number for netserver process.
  -t <num>: throughput benchmark duration in seconds. (default: ${f_duration_throughput}s)
  -l <num>: latency benchmark duration in seconds. (default: ${f_duration_latency}s)
  -f: ignore existing pid file; override and rerun.
  -w: wait for currently running benchmark to complete.
  -k: wait for the plot drawing to complete.
  -d: duration to draw the throughput time series plot.
  -z: current machine type.
  -m: mode of network test. (default: cross-region)
  -S: start netserver.
"
  exit 1
}

while getopts 'fwks:p:t:l:d:m:z:S' flag; do
  case "${flag}" in
    s) f_server="${OPTARG}" ;;
    p) f_port="${OPTARG}" ;;
    t) f_duration_throughput="${OPTARG}" ;;
    l) f_duration_latency="${OPTARG}" ;;
    f) f_force='true' ;;
    w) f_wait='true' ;;
    k) k_wait='true' ;;
    d) draw_plot_duration="${OPTARG}" ;;
    m) test_mode="${OPTARG}" ;;
    z) machine_name="${OPTARG}" ;;
    S) f_server_mode='true' ;;
    *) usage "";;
  esac
done

logdir="$HOME/$test_mode-netperf-results"
report="${logdir}/cross-region-netperf-results.log"


if [ -n "$f_wait" ];
then
  TZ=UTC-6 date -R
   exec sh -c "
    ( test -f '$logdir/success' ||
      (tail --pid \$(cat $pidfile) -f /dev/null && test -f '$logdir/success')
    ) || (echo 'Cross-region network benchmark did not complete successfully.  Check logs'; exit 1)"
fi

# if
if [ -n "$k_wait" ];
then
   TZ=UTC-6 date -R
   exec sh -c "
    ( test -f '$logdir/plot_success' ||
      (tail --pid \$(cat $pidfile) -f /dev/null && test -f '$logdir/plot_success')
    ) || (echo 'Plot for cross-region network benchmark did not complete successfully.  Check logs'; exit 1)"
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

if [[ $draw_plot_duration -gt 0 ]]; then
  exec bash -c " cd newnetperf/netperf/doc/examples && MACHINE_NAME=$machine_name TEST_MODE=$TEST_MODE DRAW_PLOT=1 DURATION=$draw_plot_duration ./runemomniaggdemo.sh "
fi

if [ -z "$f_server" ]
then
  usage "server and port args required. Use -s and -p argument to specify."
fi

rm -rf "$logdir"
mkdir "$logdir"

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
  #cd newnetperf/netperf/doc/examples && ./multistream_netperf.sh -p "$f_port" -t "$f_duration_throughput"
  cd newnetperf/netperf/doc/examples && MACHINE_NAME=$machine_name TEST_MODE=$TEST_MODE DRAW_PLOT=1 DURATION=$f_duration_throughput ./runemomniaggdemo.sh
  ) | tee "$report"



touch "$logdir/success"
