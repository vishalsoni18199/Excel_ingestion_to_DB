# Excel_ingestion_to_DB

Below are the Arguments that needs to parse while run the script

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

Example for the above arguments 

sh excel_ingestion.sh 
environment cluster 
/efs/path/to/table_DML/table.sql 
environment 
file_name 
DB_table_name  
column_list  
/user/Path/to/file 
files_name.xlsx 
mail@mail.com(whom to send) 
mail@mail.com(CC)


Below are the Arguments for the Sharepoint

ENV=$1  environmenr,\n
ENV_CLUSTER=$2  cluster,\n
url=$3 sharepoint site to generate access toke,\n
client_id - Client ID to generate access token,\n
client_secret - Client secret to generate access token,\n
SP_FILE_PATH - URL to the Excel file in sharepoint location,\n
FILENAME - Name of the target Excel file,\n
HDFS_FILE_PATH - Base HDFS path under which Excel source files & archived Excel files will be placed,\n
exit_code=0
