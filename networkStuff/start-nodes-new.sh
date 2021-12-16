#!/usr/local/bin/bash

USER=jane

LOG_FILE="tmplog.log"
LOAD_SUCCESS_FILE="load_success"
NUM_STREAMS_FILE="newnetperf/netperf/doc/examples/num_streams"
STARTED_NODES_FILE="networkStuff/started_nodes"

# STARTED_NODES_FILE is to help deleting nodes afterwards
echo > $STARTED_NODES_FILE


INSTANCE_CLOUD_GROUP_LIST=( )
INSTANCE_LIST=( )

NODES=1


temp="tmp_file"
TESTED_NODES=( )

#echo > $temp

INSTANCE_LIST_FILE="instance_list.txt"

echo > $INSTANCE_LIST_FILE

function parseJson() {

  PATHS=("$@")
  for MYPATH in ${PATHS[*]}
  do
      jq -c '.[]' $MYPATH | while read i; do
        cloud=$(echo $i | jq -r '.cloud')
        group=$(echo $i |jq -r '.group')
        machines=$(echo $i |jq -r '.machineTypes')
        while IFS='' read -r line; do
          machine=$(echo $line| sed 's/"//g')
          printf "%s:%s:%s\n" "$machine" "$group" "$cloud" >> $INSTANCE_LIST_FILE
        done < <(echo $machines | jq 'keys[]')
      done
    done
echo "finished getting instance list"
}


function waitPid() {
  PID=$1
  while kill -0 $PID 2>/dev/null;
  do sleep 1;
  done
}

function formatInstance() {
  echo $1 | sed 's/\./-/g; s/_/-/g'
}

# 1. For a machine, create east and west nodes

# create_node "c2-standard-8" "pd-ssd" "gce" "east"
function create_node() {
    local INSTANCE=$1
    local GROUP=$2
    local CLOUD=$3
    local REGION=$4
    local NODE=$USER-$(formatInstance $INSTANCE)-$GROUP-$REGION

    output=$(./cloud-report generate-create-roachprod -t $INSTANCE -g $GROUP -c $CLOUD -z $REGION -d ./cloudDetails/$CLOUD.json)
    eval $output


    while [ -z "$(roachprod list | grep $NODE)" ]
    do
      sleep 1
    done
    echo "$NODE is created"
}

# 2. Set up a node, need to wait till east and west both setup

# load_to_node "jane-c2-standard-8-east" "gce"
function load_to_node(){
  NODE=$1
  CLOUD=$2
  roachprod run "$NODE" -- tmux new -s cloud-report -d
  roachprod run "$NODE" rm -- -rf ./scripts
  roachprod put "$NODE" ./scripts scripts
  roachprod put "$NODE" ./networkStuff/network-setup.sh network-setup.sh
  roachprod run "$NODE" chmod -- -R +x ./scripts
  roachprod run "$NODE" 'rm -f ./cockroach'
  roachprod stage "$NODE" cockroach
  roachprod run "$NODE" sudo ./scripts/gen/setup.sh $CLOUD
  roachprod run "$NODE" sudo ./network-setup.sh
  roachprod run "$NODE" touch $LOAD_SUCCESS_FILE
}

# 3. Check both east and west nodes are successfully setup

# wait_till_file_exists_in_all_nodes "hello" "jane-c2-standard-8-east" "jane-c2-standard-8-west"
function wait_till_file_exists_in_all_nodes() {
  echo -n " inside wait_till_file_exists_in_all_nodes "
  local FILEPATH=$1
  shift
  local CLUSTER_LIST=("$@")

    PROC_LIST_ARR=( )

    for CLUSTER in ${CLUSTER_LIST[*]}
    do
        roachprod run "$CLUSTER":1 "until [ -f $FILEPATH ]; do sleep 1; done" &
        PROC_N=$!
        #echo "PROC_N for $FILEPATH=$PROC_N, CLUSTER=$CLUSTER"
        PROC_LIST_ARR+=( $PROC_N )
    done

    for PROC in ${PROC_LIST_ARR[*]}
    do
      waitPid $PROC
    done

    echo "--- FILE $FILEPATH EXISTS NOW IN ${CLUSTER_LIST[*]} ---"
}


# 4. modify the remote host in the east host
function get_ip() {
  CLUSTER=$1
  IP=$(roachprod run $CLUSTER:1 -- ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1')
  echo $IP
}

# modify_remote_hosts "jane-c2-standard-8-east" "jane-c2-standard-8-west"
function modify_remote_hosts() {
  # LOCAL is the one that runs netperf
  LOCAL_CLUSTER=$1
  REMOTE_CLUSTER=$2
  IP=$(get_ip $REMOTE_CLUSTER)
  FILENAME="$REMOTE_CLUSTER"_remote_hosts
  printf "REMOTE_HOSTS[0]=$IP\nREMOTE_HOSTS[1]=$IP\nNUM_REMOTE_HOSTS=2\n" >"$FILENAME"
  chmod 777 "$FILENAME"
  roachprod run "$LOCAL_CLUSTER":1 -- sudo chmod 777 -R newnetperf
  roachprod put "$LOCAL_CLUSTER":1 ./"$FILENAME" newnetperf/netperf/doc/examples/remote_hosts
}

# 5. get the best number of streams for an east node

# get_best_number_streams east-node
function get_best_number_streams() {
  local NODE=$1
  echo "running getting best num of stream for $NODE"
  roachprod run "$NODE":1 -- "cd newnetperf/netperf/doc/examples && GET_BEST_STREAM=1 ./runemomniaggdemo.sh"
  echo "get best number of stream for $NODE"
  # there should be a num_streams file now
}

# 6. check if num_streams exists in all east nodes



# 1. For an instance, create east and west nodes

# 2. Set up a node, need to wait till east and west both setup
# 3. Check both east and west nodes are successfully setup
# 4. modify the remote host in the east host
# 5. get the best number of streams for an east node
# 6. check if num_streams exists in all east nodes



# create_setup_node "c2-standard-8" "pd-ssd" "gce" "east"
function create_setup_node() {
      local INSTANCE=$1
      local GROUP=$2
      local CLOUD=$3
      local REGION=$4

#      create_node $INSTANCE $GROUP $CLOUD $REGION

      local NODE=$USER-$(formatInstance $INSTANCE)-$GROUP-$REGION

      if [ "$REGION" == "east" ]
      then
        echo "$NODE flaggggg"
        echo "$NODE" >> $temp
      fi

      echo "--------- loading ---------"

      printf "%s\n"  "$NODE" >> $STARTED_NODES_FILE

      load_to_node $NODE $CLOUD
      echo "fully loaded $NODE"
}


# preprocess_instance "c2-standard-8" "pd-ssd" "gce"
function preprocess_instance() {

  local INSTANCE=$1
  local GROUP=$2
  local CLOUD=$3

  echo "started preprocess_instance for $INSTANCE"


  local E_NODE="$USER-$(formatInstance $INSTANCE)-$GROUP-east"
  local W_NODE="$USER-$(formatInstance $INSTANCE)-$GROUP-west"


#  echo > "./networkStuff/logs/$E_NODE-$LOG_FILE"
#  echo > "./networkStuff/logs/$W_NODE-$LOG_FILE"
#
#
  create_setup_node $INSTANCE $GROUP $CLOUD "east" >> "./networkStuff/logs/$E_NODE-$LOG_FILE" 2>&1 &
  E_PROC=$!
  create_setup_node $INSTANCE $GROUP $CLOUD "west" >> "./networkStuff/logs/$W_NODE-$LOG_FILE" 2>&1 &
  W_PROC=$!
  wait $E_PROC
  wait $W_PROC

  echo "started east and west nodes for $INSTANCE-$GROUP"

#=================

  wait_till_file_exists_in_all_nodes $LOAD_SUCCESS_FILE $E_NODE $W_NODE >> "./networkStuff/logs/$E_NODE-$LOG_FILE" 2>&1
  modify_remote_hosts $E_NODE $W_NODE >> "./networkStuff/logs/$E_NODE-$LOG_FILE" 2>&1
  get_best_number_streams $E_NODE >> "./networkStuff/logs/$E_NODE-$LOG_FILE" 2>&1


}

function wait_till_all_instances_started() {

  local INSTANCES=("$@")

  echo "waiting for start of ${INSTANCES[*]}"

  for INSTANCE in ${INSTANCES[*]}
  do
    while [ -z "$(roachprod list | grep $INSTANCE )" ]
    do
      sleep 5
      done
  done
}


function run_tests() {

    for INSTANCE_CLOUD_GROUP in ${INSTANCE_CLOUD_GROUP_LIST[*]}
    do
        IFS=: read INSTANCE GROUP CLOUD <<< $INSTANCE_CLOUD_GROUP
        INSTANCE_LIST+=( $INSTANCE )
        #preprocess_instance $INSTANCE $GROUP $CLOUD &
    done

    echo "INSTANCE_LIST:${INSTANCE_LIST[*]}"

    mapfile -t TESTED_NODES < $temp
    while [ ${#TESTED_NODES[@]} -lt ${#INSTANCE_CLOUD_GROUP[@]} ]
    do
      echo "TESTED_NODES:${TESTED_NODES[*]}"
      sleep 10
      mapfile -t TESTED_NODES < $temp
      done
#
#    wait_till_all_instances_started "${INSTANCE_LIST[@]}"
#
    wait_till_file_exists_in_all_nodes $NUM_STREAMS_FILE "${TESTED_NODES[@]}"
    for MYNODE in "${TESTED_NODES[@]}"
    do
      rm -f "./networkStuff/result/$MYNODE-result.log"
      touch "./networkStuff/result/$MYNODE-result.log"
      roachprod run $MYNODE:1 -- "cd newnetperf/netperf/doc/examples && ./multistream_netperf.sh" >> "./networkStuff/result/$MYNODE-result.log" 2>&1 &
      done
    echo "------------ FINISHED -----------"
}

# ./networkStuff/get_instance_list.sh "./demoCloudDetails/azure.json" "./demoCloudDetails/aws.json" "./demoCloudDetails/gce.json"


go build -o cloud-report main.go

parseJson "./demoCloudDetails/aws.json" "./demoCloudDetails/gce.json"

while IFS= read -r line; do
  INSTANCE_CLOUD_GROUP_LIST+=("$line")
done < $INSTANCE_LIST_FILE

echo ${INSTANCE_CLOUD_GROUP_LIST[*]}

if [ ${#INSTANCE_CLOUD_GROUP_LIST[@]} -eq 0 ]; then
  echo "no instances to test!"
  exit 1
fi

run_tests




















