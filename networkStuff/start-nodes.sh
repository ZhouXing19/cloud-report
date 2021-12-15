#!/bin/bash

GCP_MACHINES=( "c2-standard-8" )
#"n2-highmem-8" "n2d-standard-8"
AWS_MACHINES=( "c5-2xlarge" "m5-2xlarge" "r5-2xlarge" )
AZURE_MACHINES=( "Standard_D8as_v4" "Standard_E8as_v4" "Standard_E8s_v4" )
PROC_LIST=proc_list
PROC_LIST_ARR=( )
BEST_STREAM_PROC_LIST_ARR=( )
LOG_FILE="load_cluster.log"
BEST_STREAM_PROCS="./best_streams"
NUM_STREAMS_PATH="/home/ubuntu/newnetperf/netperf/doc/examples/num_streams"
RESFILE="resfile"
CREATED_CLUSTER_FILE="created_cluster"

echo > $PROC_LIST
echo > $LOG_FILE
echo > $BEST_STREAM_PROCS
echo > $CREATED_CLUSTER_FILE

function loadIntoCluster(){
  CLUSTER=$1
  CLOUD=$2
  roachprod run "$CLUSTER" -- tmux new -s cloud-report -d
  roachprod run "$CLUSTER" rm -- -rf ./scripts
  roachprod put "$CLUSTER" ./scripts scripts
  roachprod put "$CLUSTER" ./networkStuff/network-setup.sh network-setup.sh
  roachprod run "$CLUSTER" chmod -- -R +x ./scripts
  roachprod run "$CLUSTER" 'rm -f ./cockroach'
  roachprod stage "$CLUSTER" cockroach
  roachprod run "$CLUSTER" sudo ./scripts/gen/setup.sh $CLOUD
  roachprod run "$CLUSTER" sudo ./network-setup.sh

}

function createCluster() {
    local INSTANCE=$1
    local CLOUD=$2
    local E_CLUSTER=jane-"$INSTANCE"-east
    local W_CLUSTER=jane-"$INSTANCE"-west

    roachprod create "$E_CLUSTER" -n 1 --lifetime 12h --clouds "$CLOUD" --gce-machine-type "$INSTANCE" --gce-zones=us-east4-c --gce-pd-volume-size=2500 --local-ssd=false --gce-pd-volume-type=pd-ssd --gce-image=ubuntu-2004-focal-v20211202 >> $LOG_FILE 2>&1 &
    E_PROC_N=$!
    roachprod create "$W_CLUSTER" -n 1 --lifetime 12h --clouds "$CLOUD" --gce-machine-type "$INSTANCE" --gce-zones=us-west1-a --gce-pd-volume-size=2500 --local-ssd=false --gce-pd-volume-type=pd-ssd --gce-image=ubuntu-2004-focal-v20211202 >> $LOG_FILE 2>&1 &
    W_PROC_N=$!
    wait $E_PROC_N
    wait $W_PROC_N

    echo $E_CLUSTER > $CREATED_CLUSTER_FILE
    echo $W_CLUSTER > $CREATED_CLUSTER_FILE
}

function setupCluster() {
  local INSTANCE=$1
  local CLOUD=$2
  local E_CLUSTER=jane-"$INSTANCE"-east
  local W_CLUSTER=jane-"$INSTANCE"-west

#  roachprod destroy "$E_CLUSTER"
#  roachprod destroy "$W_CLUSTER"

  local E_PROC_N
  local W_PROC_N



  addToPROC_LIST "$E_CLUSTER"
  addToPROC_LIST "$W_CLUSTER"

  echo "===created $E_CLUSTER and $W_CLUSTER==="

  loadIntoCluster $E_CLUSTER $CLOUD >> $LOG_FILE 2>&1 &
  E_PROC_N=$!
  loadIntoCluster $W_CLUSTER $CLOUD >> $LOG_FILE 2>&1 &
  W_PROC_N=$!
  wait $E_PROC_N
  wait $W_PROC_N

  roachprod run "$E_CLUSTER":1 touch success
  roachprod run "$W_CLUSTER":1 touch success

}

function addToPROC_LIST() {
  CLUSTER=$1
  roachprod run "$CLUSTER":1 "until [ -f ./success ]; do sleep 1; done" &
  PROC_N=$!
  echo "PROC_N=$PROC_N, CLUSTER=$CLUSTER"
  echo $PROC_N >> $PROC_LIST
}

function readProcList() {
  PROC_LIST_ARR=()
  while IFS= read -r line; do
    PROC_LIST_ARR+=("$line")
  done < $PROC_LIST
}

function add_to_best_stream_procs() {
  CLUSTER=$1
  FILEPATH=$2
  roachprod run "$CLUSTER":1 "until [ -f $FILEPATH ]; do sleep 1; done" &
  PROC_N=$!
  echo "num_streams_PROC_N=$PROC_N, CLUSTER=$CLUSTER"
  echo $PROC_N >> $BEST_STREAM_PROCS
}

function read_num_stream() {
  BEST_STREAM_PROC_LIST_ARR=()
  while IFS= read -r line; do
    BEST_STREAM_PROC_LIST_ARR+=("$line")
  done < $BEST_STREAM_PROCS
}

function getNumberOfProc() {
  nl=$(wc -l $PROC_LIST | awk '{ print $1 }')
  echo "$nl"
}

function waitPid() {
  PID=$1
  while kill -0 $PID 2>/dev/null;
  do sleep 1;
  done
}

function getIP() {
  CLUSTER=$1
  IP=$(roachprod run $CLUSTER:1 -- ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1')
  echo $IP
}

function modifyRemoteHosts() {
  # LOCAL is the one that runs netperf
  LOCAL_CLUSTER=$1
  REMOTE_CLUSTER=$2
  IP=$(getIP $REMOTE_CLUSTER)
  FILENAME="$REMOTE_CLUSTER"_remote_hosts
  printf "REMOTE_HOSTS[0]=$IP\nREMOTE_HOSTS[1]=$IP\nNUM_REMOTE_HOSTS=2\n" >"$FILENAME"
  chmod 777 "$FILENAME"
  roachprod run "$LOCAL_CLUSTER":1 -- sudo chmod 777 -R newnetperf
  roachprod put "$LOCAL_CLUSTER":1 ./"$FILENAME" newnetperf/netperf/doc/examples/remote_hosts
}

MACHINE_LIST=()

for INSTANCE in ${GCP_MACHINES[*]}
do
  E_CLUSTER=jane-"$INSTANCE"-east
  W_CLUSTER=jane-"$INSTANCE"-west
  MACHINE_LIST+=( "$E_CLUSTER" "$W_CLUSTER" )
  setupCluster $INSTANCE gce &
done


while [ "$(getNumberOfProc)" -lt "$(expr 2 \* ${#GCP_MACHINES[@]})" ]
do
  readProcList
  echo "PROC_LIST_ARR=${PROC_LIST_ARR[*]}-- waiting setupCluster to finish ..."
  sleep 5
  done

readProcList

# Wait for setup to finish
for PROC in ${PROC_LIST_ARR[*]}
do
  echo "waiting for PID $PROC to finish ..."
  waitPid $PROC
done

for INSTANCE in ${GCP_MACHINES[*]}
do
    E_CLUSTER=jane-"$INSTANCE"-east
    W_CLUSTER=jane-"$INSTANCE"-west
    modifyRemoteHosts $E_CLUSTER $W_CLUSTER

    IP=$(getIP $W_CLUSTER)
    echo "INSTANCE:$INSTANCE, IP:$IP"


    if [[ ! -z $(roachprod run "$W_CLUSTER":1 -- ps -ef | grep netserver | grep /usr/bin/netserver) ]]; then
      roachprod run "$W_CLUSTER":1 -- sudo netserver -p 12865
    fi
    echo "netserver is started at $W_CLUSTER"


    roachprod run "$E_CLUSTER":1 -- GET_BEST_STREAM=1 newnetperf/netperf/doc/examples/runemomniaggdemo.sh >> $LOG_FILE 2>&1 &
done




for INSTANCE in ${GCP_MACHINES[*]}
do
    E_CLUSTER=jane-"$INSTANCE"-east
    W_CLUSTER=jane-"$INSTANCE"-west
    echo "running getting best num of stream for $E_CLUSTER"
    roachprod run "$E_CLUSTER":1 -- GET_BEST_STREAM=1 newnetperf/netperf/doc/examples/runemomniaggdemo.sh >> $LOG_FILE 2>&1 &
    add_to_best_stream_procs $E_CLUSTER $NUM_STREAMS_PATH

done

for PROC in ${BEST_STREAM_PROC_LIST_ARR[*]}
do
  echo "waiting for PID $PROC to finish, getting best num of stream ..."
  waitPid $PROC
done

function wait_till_file_exists_in_all_nodes() {
    local CLUSTER_LIST=$1
    local FILEPATH=$2

    PROC_LIST_ARR=()

    for CLUSTER in ${CLUSTER_LIST[*]}
    do
        roachprod run "$CLUSTER":1 "until [ -f $FILEPATH ]; do sleep 1; done" &
        PROC_N=$!
        echo "PROC_N for $FILEPATH=$PROC_N, CLUSTER=$CLUSTER"
        PROC_LIST_ARR+=( PROC_N )
    done

    for PROC in ${PROC_LIST_ARR[*]}
    do
      echo "waiting for PID $PROC to finish ..."
      waitPid $PROC
    done

    echo "--- finished waiting all from PROC_LIST_ARR ---"

}



#for INSTANCE in ${GCP_MACHINES[*]}
#do
#  E_CLUSTER=jane-"$INSTANCE"-east
#  W_CLUSTER=jane-"$INSTANCE"-west
#  roachprod destroy $E_CLUSTER &
#  roachprod destroy $W_CLUSTER &
#done



# 2. write this ip to remote_hosts

exit 0
