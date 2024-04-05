#!/bin/bash

#######################################################################################################################################
# About:
# ------
# This script is used to download file from a sharepoint location to CDP Edgenode and HDFS location.
#
# Usage of this script:
# ---------------------
# Executed using Oozie workflow. The repository needs to be first deployed.
# After selecting this script in Oozie, the py script file needs to be passed as "FILES" in shell action of Oozie
# Arguments that need to be passed are:
# (1) ENV - dev, qas, or prd
# (2) ENV_CLUSTER - peanut or drona
# (3) url - Sharepoint site to generate access token
# (4) client_id - Client ID to generate access token
# (5) client_secret - Client secret to generate access token
# (6) SP_FILE_PATH - URL to the Excel file in sharepoint location
# (7) FILENAME - Name of the target Excel file
# (8) HDFS_FILE_PATH - Base HDFS path under which Excel source files & archived Excel files will be placed
#######################################################################################################################################

echo -e "sharepoint_to_cdp.sh - Execution started\n"

# Setting a temporary file path for all files generated from the script
export PYTHON_EGG_CACHE=./myeggs

#List of arguments passed to script
ENV=$1
ENV_CLUSTER=$2
url=$3
client_id=$4
client_secret=$5
SP_FILE_PATH=$6
FILENAME=$7
HDFS_FILE_PATH=$8
exit_code=0

#Fetching job run date (current date)
DATE=`date "+%Y/%m/%d"`
#FILE_PROCESS_DATE=`date "+%Y%m%d%H%M%S"`

# Edge node local home
#HOME=/efs/home/${USER}/${ENV}
HOME=/efs/home/${USER}

EDGE_FILE_PATH=${HOME}/cp_files/${FILENAME}

# Displaying all arguments passed
echo "Below are the arguments used:"
echo "--------------------------------------------------------------------------------------------------------"
echo "ENV: ${ENV}"
echo "ENV_CLUSTER: ${ENV_CLUSTER}"
echo "FILENAME: ${FILENAME}"
echo "HOME: ${HOME}"
echo "DATE: ${DATE}"
echo "Sharepoint_FILE_PATH: ${SP_FILE_PATH}"
echo "Edgenode_FILE_PATH: ${EDGE_FILE_PATH}"
echo "HDFS_FILE_PATH: ${HDFS_FILE_PATH}"
echo "--------------------------------------------------------------------------------------------------------"

# Kerberos authentication for HDFS access
kinit ${USER}@AP.CORP.CARGILL.COM -k -t /efs/home/${USER}/${USER}.keytab

#Download file from sharepoint to CDP edgenode
echo "Downloading file from sharepoint to CDP edgenode..."
/efs/home/${USER}/pyframe/bin/python sharepoint_to_cdp.py ${url} ${client_id} ${client_secret} ${SP_FILE_PATH} ${EDGE_FILE_PATH}

#Delete file from HDFS if exists
#FILEEXISTS=`hadoop fs -ls -t ${HDFS_FILE_PATH}/input/${EXCEL} | head -1 | awk '{ print $NF }'`
#if [ -z ${FILEEXISTS} ]; then
#  echo "Copying file from edgnode to HDFS..."
#else
##if [ hdfs dfs -test -e ${HDFS_FILE_PATH}/input/${FILENAME} ]; then
#  echo "File already exists in HDFS. Deleting the file before copying from edgenode..."
#  hadoop fs -rm ${HDFS_FILE_PATH}/input/${FILENAME}
#fi

#Copy file from edgnode to HDFS
hdfs dfs -put -f ${EDGE_FILE_PATH} ${HDFS_FILE_PATH}/input
if [ $exit_code -eq 0 ]; then
  echo "File successfully loaded to HDFS"
  exit 0
else
  echo "Failed to load File to HDFS"
  exit 1
fi
