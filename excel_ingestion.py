from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from datetime import datetime, timedelta

#Function to read the required sheet from Excel file as a Spark dataframe
def readExcel(file, sheet):
    try:
        print("\nExcel file name - " + file)
        print("Sheet name - " + sheet)
        
        sheetAddress = "'" + sheet + "'" + "!A1:JZ20000"
        DataFrame = spark.read.format("com.crealytics.spark.excel").option("useHeader", "true").option("dataAddress", sheetAddress).option("treatEmptyValuesAsNulls", "true").option("inferSchema", "false").load(file)
        
        #Dropping empty rows if any
        formatted_cols = (column.replace('.', '') for column in DataFrame.columns)
        DataFrame = DataFrame.toDF(*formatted_cols).dropna("all")
        
        print("Number of records in " + sheet + " sheet - " + str(DataFrame.count()))
        
        return DataFrame

    except Exception as err:
        print("\nUnexpected error [readExcel]: " + str(sys.exc_info()[0])) + ", " + str(err)
        exit(1)
        

#Function to load staging table
def stagingLoad(DataFrame, column_list, stg_db_name, stg_tbl_name):
    try:
        print("Staging DB name - " + stg_db_name)
        print("Staging table name - " + stg_tbl_name)
        print("List of columns to be loaded - " + str(column_list))

        DataFrame.select(*column_list).write.mode("overwrite").insertInto("{0}.{1}".format(stg_db_name, stg_tbl_name),overwrite=True)

        return 0

    except Exception as err:
        print("\nUnexpected error [stagingLoad]: " + str(sys.exc_info()[0])) + ", " + str(err)
        return 1


if __name__ == "__main__":

    #Creating SparkSession object
    try:
        print("\nCreating Spark session object...")
        spark = SparkSession.builder.master("yarn").appName('AUS excel ingestion').enableHiveSupport().getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set("hive.exec.dynamic.partition", "true")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        print("Spark session object created successfully")
    except Exception as err:
        print("Unexpected error [main]: " + str(sys.exc_info()[0])) + ", " + str(err)
        exit(1)

    try:
        #Getting parameters
        file = str(sys.argv[1])
        sheet_name = str(sys.argv[2])
        col_list = str(sys.argv[3]).split(",")
        target_db_prefix = str(sys.argv[4])
        target_table = str(sys.argv[5])

        #Formatting staging DB and table names
        stg_db_name = "{0}_product_cascau_eka_customer_connect".format(target_db_prefix)
        stg_tbl_name = "{0}_stage".format(target_table)

        #To read Excel sheet into Spark dataframe
        sga_data = readExcel(file, sheet_name)

        #To load dataframe into staging table
        if stagingLoad(sga_data, col_list, stg_db_name, stg_tbl_name) == 0:
            print("\nStage table - " + stg_tbl_name + " is loaded successfully")
        else:
            print("\nStage table - " + stg_tbl_name + " load has failed. Hence quiting the job")
            exit(1)
    
        spark.stop()
        exit(0)
        
    except Exception as err:
        print("\nUnexpected error [main]: " + str(sys.exc_info()[0])) + ", " + str(err)
        exit(1)
