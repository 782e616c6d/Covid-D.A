# ETL Spark.py
# Final Project:
# Discpline: Database Topics.
# Code: Matb10.

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

# Folder Creation (If necessary).

if os.path.isfile("/home/xiatsu/Brute"):
    pass  # Nothing to do.

else:
    subprocess.run(["mkdir /home/xiatsu/Brute"])

# First Step from ETL . Data Extract Process.
# Download .zip Google. Save in '/home/xiatsu/Brute'.

from urllib import request

file_url = "https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip"
file = "/home/xiatsu/Brute"

request.urlretrieve(file_url, file)

# Folder Creation (If necessary).

if os.path.isfile("/home/xiatsu/Process"):
    pass  # Nothing to do.

else:
    subprocess.run(["mkdir /home/xiatsu/Process"])

# Extract sub. file from '/home/xiatsu/Process'.

from zipfile import ZipFile

z = ZipFile("/home/xiatsu/Brute", "r")
z.extract("2020_BR_Region_Mobility_Report.csv", "/home/xiatsu/Process")
z.close()

# Second Step from ETL. Remove useless information, and formatting the data.

from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

path = "/home/xiatsu/Process/2020_BR_Region_Mobility_Report.csv"

df = spark.read.csv(path, inferSchema=True, header=True)
df = df.drop("sub_region_1", "sub_region_2", "iso_3166_2_code",
             "census_fips_code", "place_id")
df = df.selectExpr(
    "country_region_code as Code",
    "country_region as Country",
    "date as Date",
    "retail_and_recreation_percent_change_from_baseline as Retail_and_Recreation",
    "grocery_and_pharmacy_percent_change_from_baseline as Grocery_and_Pharmacy",
)
df = df.selectExpr(
    "parks_percent_change_from_baseline as Parks",
    "transit_stations_percent_change_from_baseline as Transit_Stations",
    "workplaces_percent_change_from_baseline as Workplaces",
    "residential_percent_change_from_baseline as Residential",
)

# Final Export

# Folder Creation (If necessary).

if os.path.isfile("/home/xiatsu/Final"):
    pass  # Nothing to do.

else:
    subprocess.run(["mkdir /home/xiatsu/Final"])

df = df.write.mode("overwrite").csv("/home/xiatsu/Final")

# Show result.

# df.show()
