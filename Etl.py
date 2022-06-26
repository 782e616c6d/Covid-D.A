# Final Project.
# Discpline: Database Topics.
# Code: Matb10.

# Start the Master and workers.
# $SPARK_HOME/sbin/start-all.sh

# Stop the Master and workers.
# $SPARK_HOME/sbin/stop-all.sh

# Start SPARK SHELL with Python.
# $SPARK_HOME/bin/pyspark

# Submit .py to SPARK SHELL with Python Shell.
# $SPARK_HOME/bin/spark-

#  Libraries to run operating system commands through Python.

print("Importing libraries to run OS commands through Python.")

import subprocess
import os
import sys

# Download and install external Findspark and Pandas packages if needed.

print(
    "Download and installation of external packages Findspark and Pandas in progress, if the packages are installed this step will be automatically skipped."
)

subprocess.run(["pip", "install", "findspark"])
subprocess.run(["pip", "install", "pandas"])

# Findspark and Pandas libraries.

print("Importing Findspark and Pandas.")

import findspark

findspark.init()

import pyspark
import pandas as pd

# First Step from ETL. Data Extract Process.

# Folder Creation (If necessary).

if os.path.isdir("/opt/Brute"):
    pass  # Nothing to do.

else:
    subprocess.run(["mkdir", "/opt/Brute"])

# Download .zip Google Community Mobility Reports. Save in '/opt/Brute'.

print("Downloading Google Community Mobility Reports...")

from urllib import request

file_url = "https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip"
file = "/opt/Brute/Region_Mobility_Report_CSVs.zip"

request.urlretrieve(file_url, file)

# Folder Creation (If necessary).

if os.path.isdir("/opt/Processing"):
    pass  # Nothing to do.

else:
    subprocess.run(["mkdir", "/opt/Processing"])

# Download Cases.csv from the Fiocruz/eSUS-VE database. Save in '/opt/Processing'.

print("Downloading Cases Reports...")

file_url = "https://raw.githubusercontent.com/Xiatsus/Xiatsus-Task-Unit/main/Database/Fiocruz%20Database/Cases.csv"
file = "/opt/Processing/Cases.csv"

request.urlretrieve(file_url, file)

# Download Deaths.csv from the Fiocruz/SIVEP-Gripe database. Save in '/opt/Processing'.

print("Downloading Death Reports...")

file_url = "https://raw.githubusercontent.com/Xiatsus/Xiatsus-Task-Unit/main/Database/Fiocruz%20Database/Deaths.csv"
file = "/opt/Processing/Deaths.csv"

request.urlretrieve(file_url, file)

# Extract sub. file from '/opt/Processing'.

print("Extracting mobility reports referring to the Br community.")

from zipfile import ZipFile

z = ZipFile("/opt/Brute/Region_Mobility_Report_CSVs.zip", "r")
z.extract("2020_BR_Region_Mobility_Report.csv", "/opt/Processing")
z.close()

# Second Step from ETL. Remove useless information, and formatting the data.

print("Starting data processing...")

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext

# Create SparkSession

spark = SparkSession.builder.master("local").appName("Etl.py").getOrCreate()

#or

# spark = SparkSession.builder.getOrCreate()

# Processing: Google Community Mobility Reports.

path = "/opt/Processing/2020_BR_Region_Mobility_Report.csv"

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

# Exporting .csv.

df = df.write.option(
    "header", True).mode('overwrite').csv("/opt/Processing/Mobility_Report.csv")

print(
    "Google Community Mobility Reports has been processed, and saved in the directory /opt/Processing."
)

# Processing: Cases Reports.

path1 = "/opt/Processing/Cases.csv"

df1 = pd.read_csv(path1, header=None, nrows=362, index_col=0)
df1 = df1.transpose()
df1.columns = ["Date", "Cases"]

# Max = Line 362.

# Exporting .csv.

df1 = df1.to_csv("/opt/Processing/Cases.csv",
                 header=True,
                 index=False,
                 index_label=False)

print(
    "Cases Reports has been processed, and saved in the directory /opt/Processing."
)

# Processing: Deaths Reports.

path2 = "/opt/Processing/Deaths.csv"

df2 = pd.read_csv(path2,
                  header=None,
                  nrows=289,
                  index_col=0,
                  on_bad_lines='skip')
df2 = df2.transpose()
df2.columns = ["A", "B", "C", "D", "E", "F", "G", "H"]
df2 = df2.drop(columns=["C", "D", "E", "F", "G", "H"])
df2.columns = ["Date", "Occurrences"]

# Max = Line 289.

# Exporting .csv.

df2 = df2.to_csv("/opt/Processing/Deaths.csv",
                 header=True,
                 index=False,
                 index_label=False)

print(
    "Deaths Reports has been processed, and saved in the directory /opt/Processing."
)

# Final export occurs at the end of processing each of the .Csv / Data Sources.

print("Starting merging of .Csv Dataframes...")

# Folder Creation (If necessary).

if os.path.isdir("/opt/Final"):
    pass  # Nothing to do.

else:
    subprocess.run(["mkdir", "/opt/Final"])

path = "/opt/Processing/Mobility_Report.csv"
path1 = "/opt/Processing/Cases.csv"
path2 = "/opt/Processing/Deaths.csv"

df = spark.read.csv(path, inferSchema=True, header=True)
df1 = spark.read.csv(path1, inferSchema=True, header=True)
df2 = spark.read.csv(path2, inferSchema=True, header=True)

df3 = df1.join(df2, on=["Date"]).orderBy("Date")

df3 = df3.coalesce(1).write.option(
    "header", True).mode('overwrite').csv("/opt/Processing/Almost.csv")

path3 = "/opt/Processing/Almost.csv"

df4 = spark.read.csv(path3, inferSchema=True, header=True)

df4 = df4.join(df, on=["Date"]).orderBy("Date")

df4 = df4.coalesce(1).write.option(
    "header", True).mode('overwrite').csv("/opt/Final/Ready.csv")

# Last step.

while True :
	choice = input("Throughout the process directories were created, do you want to keep all of them? (Y/N).")
	if choice == "Y" or choice == "y" or choice == "Yes" or choice == "yes":
        subprocess.run(["rm", "-rf", "/opt/Processing/Almost.csv"])
		print("All directories created will be kept, their location is (/opt/).\nThe data was processed, saved, and merged into a single file, located at (/opt/Final/Ready.csv), for the analysis step, access your preferred BI tool, and import the data.")

		break
	elif choice == "N" or choice == "n" or choice == "No" or choice == "no":
	    subprocess.run(["rm", "-rf", "/opt/Brute"])
        subprocess.run(["rm", "-rf", "/opt/Processing"])
		print("Only /opt/Final/Ready.csv will be kept.\nThe data was processed, saved, and merged into a single file, located at (/opt/Final/Ready.csv), for the analysis step, access your preferred BI tool, and import the data.")

		break
	else:
		print("Accepted entries are Y or N.")

		pass

# Show result. It can be used for testing purposes in any part of the operation with Dataframes:

# df.show()