#!/usr/bin/bash

############################## sample trigger ################################################################
# <K26-ingestion.sh> <K26-ingestion> <InitialLoad/CDC> <True/False>
############################## sample trigger ################################################################

aws_account=$(aws sts get-caller-identity --query Account --output text)
aws_account=$(echo $aws_account|awk '{print $1}')
region=$(ec2-metadata --availability-zone | sed 's/placement: \(.*\).$/\1/')
region=$(echo $region|awk '{print $1}')
enviroment=$(aws ec2 describe-vpcs --region ${region} --filters "Name=tag:Application,Values=Default" --query 'Vpcs[].Tags[?Key==`Environment`].Value' --output text)
enviroment=$(echo $enviroment|awk '{print $1}')

  retVal=$?
  if [ $retVal -ne 0 ]; then
      echo $(date +'%Y-%m-%d_%H-%M-%S:') "FAILED to retrieve AWS account number with RC=$retVal"
      echo "------------------------------------------------------"
      exit $retVal
  fi

json_filename=$1
run_mode=$2
cluster_size='large' #$3
step_conncurrency='10'  #$4

#run the EMR data ingestion script for K26 tables
cd /Scripts/deltalake_cdc_datapipeline/src/orchestration/emr_execute_script/src
python3 inflate_emr_ingestion.py --env ${enviroment} --acct ${aws_account} --emr_name ${json_filename} --cluster_size ${cluster_size} --config_file <s3-${aws_account}-codedeployment-bucket-${enviroment}-bucket>@deltalake_cdc_datapipeline/src/ingestion/config/${enviroment}/${json_filename}.json --run_mode ${run_mode} --step_conncurrency ${step_conncurrency}
