# -*- coding: utf-8 -*-
"""ETL Spark.py
Final Project :
Discpline: Database Topics.
Code: Matb10.
"""
# Start the Master and workers.
# $SPARK_HOME/sbin/start-all.sh

# Stop the Master and workers.
# $SPARK_HOME/sbin/stop-all.sh

# Start SPARK SHELL with Python.
# $SPARK_HOME/bin/pyspark

# Submit .py to SPARK SHELL with Python Shell.
# $SPARK_HOME/bin/spark-submit

# Libraries to run operating system commands through Python.

import subprocess
import os
import sys

import findspark

findspark.init()

import pyspark

# First Step from ETL. Data Extract Process.

# Folder Creation (If necessary).

if os.path.isdir("/home/usr/abc"):
    pass  # Nothing to do.

else:
    subprocess.run(["mkdir", "/home/usr/abc"])

# Download .zip Google Community Mobility Reports. Save in '/home/usr/abc'.

from urllib import request

file_url = "https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip"
file = "/home/usr/abc/Region_Mobility_Report_CSVs.zip"

request.urlretrieve(file_url, file)

# Folder Creation (If necessary).

if os.path.isdir("/home/usr/def"):
    pass  # Nothing to do.

else:
    subprocess.run(["mkdir", "/home/usr/def"])

# Download Cases.csv from the Fiocruz/eSUS-VE database. Save in '/home/usr/def'.

file_url = "https://raw.githubusercontent.com/Xiatsus/Xiatsus-Task-Unit/main/Database/Fiocruz%20Database/Cases.csv"
file = "/home/usr/def/Cases.csv"

request.urlretrieve(file_url, file)

# Download Deaths.csv from the Fiocruz/SIVEP-Gripe database. Save in '/home/usr/def'.

file_url = "https://raw.githubusercontent.com/Xiatsus/Xiatsus-Task-Unit/main/Database/Fiocruz%20Database/Deaths.csv"
file = "/home/usr/def/Deaths.csv"

request.urlretrieve(file_url, file)

# Extract sub. file from '/home/usr/def'.

from zipfile import ZipFile

z = ZipFile("/home/usr/abc/Region_Mobility_Report_CSVs.zip", "r")
z.extract("2020_BR_Region_Mobility_Report.csv", "/home/usr/def")
z.close()

# Second Step from ETL. Remove useless information, and formatting the data.

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext

# Create SparkSession

# spark = SparkSession.builder.master("local").appName("Etl").getOrCreate()

spark = SparkSession.builder.getOrCreate()

path = "/home/usr/def/2020_BR_Region_Mobility_Report.csv"

df = spark.read.csv(path, inferSchema=True, header=True)
df = df.drop("sub_region_1", "sub_region_2", "iso_3166_2_code",
             "census_fips_code", "place_id")
df = df.selectExpr(
    "country_region_code as Code",
    "country_region as Country",
    "date as Date",
    "retail_and_recreation_percent_change_from_baseline as Retail_and_Recreation",
    "grocery_and_pharmacy_percent_change_from_baseline as Grocery_and_Pharmacy",
    "parks_percent_change_from_baseline as Parks",
    "transit_stations_percent_change_from_baseline as Transit_Stations",
    "workplaces_percent_change_from_baseline as Workplaces",
    "residential_percent_change_from_baseline as Residential",
)

# Final Export

# Folder Creation (If necessary).

if os.path.isdir("/home/usr/ghi"):
    pass  # Nothing to do.

else:
    subprocess.run(["mkdir", "/home/usr/ghi"])

#  Exporting .csv with header.

df = df.write.option(
    "header", True).mode('overwrite').csv("/home/usr/ghi/Mobility_Report.csv")

# Show result - Test.

# df.show()