# pyspark_sample_project

This project automates the processing of CSV files on a Linux system using PySpark. 

It consists of a PySpark script (main.py) that reads incoming CSV files from an input directory, applies transformations such as renaming columns and filtering, then stores the transformed data in Parquet format in an output directory. The original CSV files are archived in an archive folder.

 A cron job is set up to run the script every minute, ensuring that the process runs automatically. 
 
 Additionally, the generate_files.py script generates sample CSV files every minute to simulate incoming data. The system is designed for hands-free, recurring data processing with minimal manual intervention, with logs captured in logfile.log for monitoring.