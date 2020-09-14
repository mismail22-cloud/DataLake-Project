# Project Overview:
Building Music Inventory Data warehouse dimensional Model implemented by AWS EMR (Elastic Map Reduce) Cluster that process the data in pyspark dataframes

# Purpose of Music Inventory Data warehouse:
As a Startup , Sparkify needs to get the full power of Information and Insights that can be got from data.This Insights can provide Sparkify with knowledge over Users Usages ,their preferences , locations & top demanded songs so that Sparkify Managment can take the right decisions on the right time to be able to act correctly towards the market.
This DWH will serve the need to avail Sparkify Managment with the Required Insights and Information so that they can take the optimum decisions.


# Data warehouse solution design:

## 1.The Data warehouse is hosted on AWS Cloud S3 Storage in Parquet file format.
This helps to reduce the upfront Infrastructure costs for Sparkify.
Also will help to pay as per the resources consumed by Sparkify.
Adding to that the flexibility to Scale up or down according to the Data sizes and traffic loads that may differ from time period to another.
Noting that this will be very beneficial in case of Startup Company like Sparkify.

## 2.Database Model:
### a.The Model is Dimensional Star Schema Model consisting of one Fact Table "SONGPLAY" surrounded by Dimensions "SONG","ARTIST","USER","TIME".This Helps to have high performance of the analytical queries that will run on top of this dimensional model.


## 3.ETL Solution 
The ETL Solution is implemented by AWS EMR (Elastic Map Reduce) Cluster that process the data in pyspark dataframes summarized in the below:
### a.Reading the System JSON files 'Songs' & 'Log' files from S3 Bucket into EMR (Elastic Mao Reduce Cluster) Spark dataframes
### b.Transforming the Data to the Dimensional Model Tables.
### c.Populating the Model back to S3 Bucket in the form of parquet files
