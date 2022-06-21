# -*- coding: utf-8 -*-
"""ETL Spark.py
Final Project :
Discpline: Database Topics.
Code: Matb10.
"""

# Libraries to run operating system commands through python.

import subprocess
import os
import sys
import findspark

findspark.init("spark-3.3.0-bin-hadoop3")

import pyspark

# Folder Creation (If necessary).

if os.path.isfile("/home/xiatsu/Brute"):
    pass  # Nothing to do.

else:
    subprocess.run(["mkdir /home/xiatsu/Brute"])

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

# First step from ETL. Data extract, rename columns, and remove unseless information.

from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

path = "/home/xiatsu/Process/2020_BR_Region_Mobility_Report.csv"

df = spark.read.csv(path, inferSchema=True, header=True)
df = df.drop(
    "sub_region_1", "sub_region_2", "iso_3166_2_code", "census_fips_code", "place_id"
)
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

# Show result.

df.show()
