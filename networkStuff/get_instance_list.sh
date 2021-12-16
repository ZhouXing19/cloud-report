#!/bin/zsh

INSTANCE_LIST="instance_list.txt"
echo > $INSTANCE_LIST

function parseJson() {

  PATHS=("$@")
  for MYPATH in ${PATHS[*]}
  do
      local e_region
      local w_region
      jq -c '.[]' $MYPATH | while read i; do
        cloud=$(echo $i | jq -r '.cloud')
        group=$(echo $i |jq -r '.group')
        case $cloud in
        aws)
          e_region=$(echo $i | jq -r '.roachprodArgs."aws-zones"')
          w_region=$(echo $i | jq -r '.roachprodArgs."west-aws-zones"')
          ;;
        azure)
          e_region=$(echo $i | jq -r '.roachprodArgs."azure-locations"')
          w_region=$(echo $i | jq -r '.roachprodArgs."west-azure-locations"')
          ;;
        gce)
          e_region=$(echo $i | jq -r '.roachprodArgs."gce-zones"')
          w_region=$(echo $i | jq -r '.roachprodArgs."west-gce-zones"')
          ;;
        *)
          echo "cloud \"$cloud\" is not supported"
          exit 1
          ;;
        esac
        machines=$(echo $i |jq -r '.machineTypes')
        while IFS='' read -r line; do
          machine=$(echo $line| sed 's/"//g')
          echo -n "$machine:$cloud:$group:$e_region:$w_region\n" >> $INSTANCE_LIST
        done < <(echo $machines | jq 'keys[]')
      done
    done

}

parseJson "$@"

#"./demoCloudDetails/azure.json" "./demoCloudDetails/aws.json" "./demoCloudDetails/gce.json"

#IFS=: read machineInstnace machineCloud machineGroup machineERegion machineWRegion <<<  "${INSTANCE_LIST[2]}"