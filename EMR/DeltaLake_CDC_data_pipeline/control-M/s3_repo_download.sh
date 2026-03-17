#!/usr/bin/bash

#getting current aws account
aws_account=$(aws sts get-caller-identity --query Account --output text)
aws_account=$(echo $aws_account|awk '{print $1}')
region=$(ec2-metadata --availability-zone | sed 's/placement: \(.*\).$/\1/')
region=$(echo $region|awk '{print $1}')
enviroment=$(aws ec2 describe-vpcs --region ${region} --filters "Name=tag:Application,Values=Default" --query 'Vpcs[].Tags[?Key==`Environment`].Value' --output text)
enviroment=$(echo $enviroment|awk '{print $1}')
#project_name=project_name
# added parameter to the script to make to reusable
project_name=${1:-deltalake_cdc_data_pipeline}
echo $project_name


  retVal=$?
  if [ $retVal -ne 0 ]; then
      echo $(date +'%Y-%m-%d_%H-%M-%S:') "FAILED to retrieve AWS account number with RC=$retVal"
      echo "------------------------------------------------------"
      exit $retVal
  fi

#delete exsiting project from local
echo "rm -rf ${project_name}"
rm -rf ${project_name}

#download source codes from S3 to local directory
echo $(date +'%Y-%m-%d_%H-%M-%S:') "aws s3 cp s3://s3-${aws_account}-codedeployment-bucket-${enviroment}/${project_name} /Scripts/${project_name} --recursive "
#To solve the project download issue into control-M, added relative path /Scripts/
# into destination directory in aws copy command
aws s3 cp s3://s3-${aws_account}-codedeployment-bucket-${enviroment}/${project_name}/ /Scripts/${project_name} --recursive
chmod -R 740 /Scripts/${project_name}/
# Added return code for aws copy operation. This will help ec2 instance to get the execution status of script
  retVal=$?
  if [ $retVal -ne 0 ]; then
      echo $(date +'%Y-%m-%d_%H-%M-%S:') "FAILED to download project from S3 bucket RC=$retVal"
      echo "------------------------------------------------------"
      exit $retVal
  fi