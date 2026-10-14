################################################################
# Developed By:Hosea Mutwiri Kinyua                            #
# Developed Date: 16-08-2026                                   # 
# Script Name:run_presc_pipeline_project.ksh                   #
# PURPOSE: Master Script to run the entire project end to end. #
################################################################



PRO_FOLDER="/home/${USER}/prescriber_pipeline/src/main/python"


### Part 1
### Call the copy_to_hdfs wrapper to copy the input vendor files from Local to HDFS..

printf "\nCalling copy_files_local_to_hdfs.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ... \n"

${PRO_FOLDER}/bin/copy_files_hdfs_to_local.ksh

printf "Executing copy_files_local_to_hdfs.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"


### Call below wrapper to delete HDFS Paths.

printf "Calling delete_hdfs_output_paths.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ... \n"

${PRO_FOLDER}/bin/delete_hdfs_output_path.ksh

#echo "Executing delete_hdfs_output_paths.ksh is completed."

printf "Executing delete_hdfs_output_paths.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"


### Call below Spark Job to extract Fact and City Files

printf "Calling run_presc_pipeline.py at `date +"%d/%m/%Y_%H:%M:%S"` ...\n"
spark-submit --master yarn --num-executors 28 --jars ${PRO_FOLDER}/lib/postgresql-42.3.5.jar run_presc_pipeline.py
printf "Executing run_presc_pipeline.py is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"


### Part 2
### Call below script to copy files from HDFS to local.
printf "Calling copy_files_hdfs_to_local.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ...\n"
${PRO_FOLDER}/bin/copy_files_hdfs_to_local.ksh
printf "Executing copy_files_hdfs_to_local.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"

### Call below script to copy files to S3.
printf "Calling copy_files_to_s3.ksh at `date +"%d/%m/%Y_%H:%M:%S"` ...\n"
${PRO_FOLDER}/bin/copy_files_to_s3.ksh
printf "Executing copy_files_to_s3.ksh is completed at `date +"%d/%m/%Y_%H:%M:%S"` !!! \n\n"




######################################## THE END ###############################################
