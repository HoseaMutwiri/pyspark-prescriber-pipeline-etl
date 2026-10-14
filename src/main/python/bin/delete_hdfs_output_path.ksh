############################################################
# Developed By:                                            #
# Developed Date:                                          # 
# Script NAME:                                             #
# PURPOSE: Delete HDFS Output paths so that Spark 
#          extraction will be smooth.                      #
############################################################

# Declare a variable to hold the unix script name.
JOBNAME="delete_hdfs_output_paths.ksh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

#Define a Log File where logs would be generated
LOGFILE="/home/hoseamutwirikinyua/prescriber_pipeline/src/main/python/logs/delete_hdfs_output_paths_${date}.log"

###########################################################################
### COMMENTS: From this point on, all standard output and standard error will
###           be logged in the log file.
###########################################################################
{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"

CITY_PATH=/PrescPipeline/output/dimension_city
hdfs dfs -test -d $CITY_PATH
status=$?
if [ $status == 0 ]
  then
  echo "The HDFS output directory $CITY_PATH is available. Proceed to delete."
  hdfs dfs -rm -r -f $CITY_PATH
  echo "The HDFS Output directory $CITY_PATH is deleted before extraction."
fi

FACT_PATH=/PrescPipeline/output/presc
hdfs dfs -test -d $FACT_PATH
status=$?
if [ $status == 0 ]
  then
  echo "The HDFS output directory $FACT_PATH is available. Proceed to delete."
  hdfs dfs -rm -r -f $FACT_PATH
  echo "The HDFS Output directory $FACT_PATH is deleted before extraction."
fi

echo "${JOBNAME} is Completed...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.

