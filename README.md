## Problem Statement:

Your goal is to write a Python program that will download a zip file from a given URL, extract the
JSON files from the zip, parse the data from the JSON files, store the data in HDFS, and view the data
through HIVE. You will need to use the requests, zipfile, and json libraries to accomplish this.
Specifically, you should use the requests library to download the zip file, the zipfile library to extract
the JSON files and parse the data from them, and the json library to parse the data from the JSON
files. Once you have parsed the data, you can use the hadoop command-line utility or the WebHDFS
REST API to store the data in HDFS, and use the hive command-line interface or a HIVE client to
connect to the HIVE server and query and view the data.

## Getting the Data

Using Boto3 API, I've created an s3 bucket to store the zipfile. Using Requests module I got the zipfile from the url through get method in a variable. I didn't write it into my local. Using s3 put_object method I put it in the bucket directly.

## Putting the data in HDFS

I created an EMR cluster with Hadoop, Hive, Spark, Livy and JupyterEnterpriseGateway. And I connected with the cluster from EMR notebook. Using the Subprocess module I executed hdfs commands in EMR cluster from the notebook. I created a directory named sec_data inside hadoop directory to put the json files. And I copied all the json files from the s3 bucket to hdfs.

## Processing the json files in PySpark

I created a spark session. And I've configured the spark session in a way that it uses hive metastore as its default metastore and the hive warehouse location. As per the requirement data need to stored in Hive.

I've read the firt json file as a dataframe and printed the schema. The json file has 14 columns and all the columns are array type with 1000s of elements. So, first I needed to convert it into row format to put it into the database as a table. So, I exploded all the columns, now the dataframe looks better.

Through Spark sql I could access Hive. Using spark createOrReplaceTempView() method I created a temperory table out of the dataframe. Basesd on that temperary table I created another table named sec_table. Using the write function I inserted the dataframe into the hive table.

All the json files need to be processed this way. For that I created list of json file names. By looping through the list I read the json files and transformed and put it into the hive table.

## Querying the table

I queried the hive table through Pyspark as well as hive commands.
