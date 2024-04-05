#!/bin/bash

#######################################################################################################################################
# About: 
# ------
# This script is used to ingest Customer Profitability - Excel sheet for various countries into Analytics database.
#
# Usage of this script:
# ---------------------
# Executed using Oozie workflow. The repository needs to be first deployed.
# After selecting this script in Oozie, the Impala query file & py script file needs to be passed as "FILES" in shell action of Oozie
# Also store the queries in this repository. Path: CDP_SEA_DATA_ORG > bin > customer_profitability > excel_ingestion > impala_queries
# Arguments that need to be passed are:
# (1) ENV - dev, qas, or prd
# (2) ENV_CLUSTER - peanut or drona
# (3) IMPALA_SCRIPT_FILE_NAME - Queries that will be executed to load target table from staging table.
# (4) TARGET_DB_PREFIX - This is the target database prefix. e.g. dev, qas, or prd
# (5) SHEETNAME - Sheet name to be ingested
# (6) COLUMNLIST - List of columns to be ingested
# (7) HDFS_FILE_PATH - Base HDFS path under which Excel source files & archived Excel files will be placed
# (8) EXCEL - Name of the excel file
# (9) EMAIL_TO - Email address (To) for notifications. Multiple emails separated by commas
# (10) EMAIL_CC - Email address (CC) for notifications. Multiple emails separated by commas
# (11) TARGET TABLE - Target table to load
#######################################################################################################################################


echo -e "excel_ingestion.sh - Execution started\n"

# Setting a temporary file path for all files generated from the script
export PYTHON_EGG_CACHE=./myeggs

#List of arguments passed to script
ENV=$1
ENV_CLUSTER=$2
IMPALA_SCRIPT_FILE_NAME=$3
TARGET_DB_PREFIX=$4
SHEETNAME=$5
TARGET_TABLE=$6
COLUMNLIST=$7
HDFS_FILE_PATH=$8
EXCEL=$9
EMAIL_TO=${10}
EMAIL_CC=${11}
PSID=$USER
exit_code=0

#Fetching job run date (current date)
DATE=`date "+%Y/%m/%d"`


# Edge node local home
HOME=/efs/home/${USER}/


# Displaying all arguments passed
echo "Below are the arguments used:"
echo "--------------------------------------------------------------------------------------------------------"
echo "ENV: ${ENV}"
echo "ENV_CLUSTER: ${ENV_CLUSTER}"
echo "IMPALA_SCRIPT_FILE_NAME: ${IMPALA_SCRIPT_FILE_NAME}"
echo "TARGET_DB_PREFIX: ${TARGET_DB_PREFIX}"
echo "HOME: ${HOME}"
echo "DATE: ${DATE}"
echo "PSID: ${PSID}"
echo "EMAIL_TO: ${EMAIL_TO}"
echo "EMAIL_CC: ${EMAIL_CC}"
echo "--------------------------------------------------------------------------------------------------------"

# Kerberos authentication for HDFS access
kinit ${USER}@AP.CORP.CARGILL.COM -k -t /efs/home/${USER}/${USER}.keytab

#Checking file availability (If file is placed in NAS path)
#FILENAME=`find /efs/home/<ps_id>/<file_path>/ -maxdepth 1 -type f -name ${FILE_CHECK}`
#FILENAME=`ls -lt /efs/home/<ps_id>/<file_path>/${FILE_CHECK} 2> /dev/null | head -1 | awk '{ print $NF }'`

#Checking file availability (If file is placed in HDFS path)
FILENAME=`hadoop fs -ls -t ${HDFS_FILE_PATH}/${EXCEL} | head -1 | awk '{ print $NF }'`


if [ -z ${FILENAME} ]; then
	echo -e "\nExcel source file not available. Hence quiting the job."
	exit_code=2
else
  #ARCHIVED=`hadoop fs -ls -t ${HDFS_FILE_PATH}/archive/_* | grep ${EXCEL%.*} | head -1`
  echo "\nARCHIVED Path: $ARCHIVED"

  #CHECKSUM="`diff -s <(hadoop fs -checksum ${FILENAME} | cut -f3) <(hadoop fs -checksum ${ARCHIVED##* } | cut -f3) | grep -c 'are identical'`"
  echo "\nCHECKSUM: $CHECKSUM"

  #if [ ${CHECKSUM} -eq 1 ]; then
    echo -e "\nExcel already processed. Hence quiting the job."
  	exit_code=3
  #else
    echo -e "\nNew Excel source file - ${FILENAME} is available. Hence starting below spark job:"
    echo "--------------------------------------------------------------------------------------------------------"
    echo "SPARK_MAJOR_VERSION=2 spark-submit --master "yarn" --deploy-mode "cluster" --conf spark.ui.port=9999 --packages com.crealytics:spark-excel_2.11:0.11.1 excel_ingestion.py  ${FILENAME} "${SHEETNAME}" "${COLUMNLIST}" ${TARGET_DB_PREFIX} ${TARGET_TABLE}"
    echo "--------------------------------------------------------------------------------------------------------"

    SPARK_MAJOR_VERSION=2 spark-submit --master "yarn" --deploy-mode "cluster" --conf spark.ui.port=9999 --packages com.crealytics:spark-excel_2.11:0.11.1 excel_ingestion.py ${FILENAME} "${SHEETNAME}" "${COLUMNLIST}" ${TARGET_DB_PREFIX} ${TARGET_TABLE}


    #Based on spark job's exit status, calling Impala command to load target table
    if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
      #Command to execute Impala queries
      impala-shell --var=target_db_prefix=${TARGET_DB_PREFIX} --var=psid=${PSID} -k --ssl -i ${ENV_CLUSTER}-impala.cargill.com -f "${IMPALA_SCRIPT_FILE_NAME}"
      rc=$?

      if [ $rc -eq 0 ]; then
        echo -e "\nTarget tables load is completed successfully."
        echo -e "\nexcel_ingestion.sh - Excel sheet ingestion job successful."

        #Archival process
        #if ls ${FILENAME} 1> /dev/null 2>&1; then
        if hadoop fs -ls ${FILENAME} 1> /dev/null 2>&1; then
          echo -e "\nArchiving Excel file - ${FILENAME}"
          #mv -f ${FILENAME} /efs/home/ps040123/excel_ingestion_dev/archive
          FILENAME_TEMP=${FILENAME##*/}
          FILENAME_ARCHIVE=${ENV}_${FILENAME_TEMP%.*}.xlsx
          hadoop fs -mv ${FILENAME} ${HDFS_FILE_PATH}/archive/${FILENAME_ARCHIVE}
          echo -e "Excel file archival is completed"

          hdfs dfs -rm -r -skipTrash `hdfs dfs -ls ${HDFS_FILE_PATH}/archive | awk '$6 <= "'$(date -d '22 days ago' +%Y-%m-%d)'" {print $NF}'`
          echo -e "Purge: 22 days older archived excels deleted"
        fi

        exit_code=0
      else
        echo -e "\nexcel_ingestion.sh - Excel sheet ingestion job has failed during target table load."
        exit_code=1
      fi
    else
      echo -e "\nexcel_ingestion.sh - Excel sheet ingestion job has failed during staging table load."
      exit_code=1
    fi
  #fi
fi

#Email block
if [ $exit_code -eq 0 ]; then
	echo -e "Hi All,\\n\\nExcel sheet ingestion is successful. Please find the details below:\\n\\nFile name - ${FILENAME}\\nJob run date - ${DATE}\\n\\nThank you." | mailx -s "CASC APAC Data Analytics - Excel sheet ingestion for ${ENV^^} - Success" -c "${EMAIL_CC}" -r "donotreply@cargill.com" "${EMAIL_TO}"
	exit 0
elif [ $exit_code -eq 2 ]; then
	echo -e "Hi All,\\n\\nExcel sheet ingestion has failed. Excel source file not available. Hence quiting the job. Please find the details below:\\n\\nFile name - ${FILENAME}\\nJob run date - ${DATE}\\n\\nThank you." | mailx -s "CASC APAC Data Analytics - Excel sheet ingestion for ${ENV^^} - Failure" -c "${EMAIL_CC}" -r "donotreply@cargill.com" "${EMAIL_TO}"
	exit 2
elif [ $exit_code -eq 3 ]; then
	echo -e "Hi All,\\n\\nExcel sheet already processed. Hence quiting the job. Please find the details below:\\n\\nFile name - ${FILENAME}\\nJob run date - ${DATE}\\n\\nThank you." | mailx -s "CASC APAC Data Analytics - Excel sheet ingestion for ${ENV^^} - Success" -c "${EMAIL_CC}" -r "donotreply@cargill.com" "${EMAIL_TO}"
	exit 0
else
	echo -e "Hi All,\\n\\nExcel sheet ingestion has failed. Please find the details below:\\n\\nFile name - ${FILENAME}\\nJob run date - ${DATE}\\n\\nThank you." | mailx -s "CASC APAC Data Analytics - Excel sheet ingestion for ${ENV^^} - Failure" -c "${EMAIL_CC}" -r "donotreply@cargill.com" "${EMAIL_TO}"
	exit 1
fi
