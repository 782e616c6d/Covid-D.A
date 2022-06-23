

Final Project.

Project for academic purposes.


![spark-logo-rev](https://user-images.githubusercontent.com/76137086/174940667-b6b5f635-71a4-434d-8e1b-e9c8e83acee0.svg)


1 - Install Apache Spark dependencies.
Proceed with installing it.

2 - Install Apache Spark. (Im opted for the "Stand Alone Cluster" Mode, as it suited me, but feel free to check and suggest other simpler and more efficient installation modes).

3 - If, like me, you chose the "Stand Alone" mode, follow the steps in the documentation.

Doc. Link:

https://spark.apache.org/docs/latest/spark-standalone.html

4 - After installation, the cluster will be able to run and perform its proper functions, run your startup script "start-all.sh" and wait. (Access can be done via WebUi, or through the terminal, at your discretion).

5 - With the cluster in full operation, submit your applications through "spark-submit".

6 - To shut down the entire cluster, run "stop-all.sh".


![0_Dnt6wUWlARdI1wim](https://user-images.githubusercontent.com/76137086/174943043-f9a2b98b-a2eb-41db-a167-9db342350dda.png)


Link to databases:

1 - Community Mobility Reports (Br).
Database: Google.
Link: https://www.google.com.br/covid19/mobility/
 
2 - Variation of Cases (Covid-19).
Database: Fiocruz.
Link: https://bigdata-covid19.icict.fiocruz.br/

Period: January/2020 - December/2020.

However, it can be easily extrapolated, due to constant data updates.


![0](https://user-images.githubusercontent.com/76137086/174943501-d5fd7b9d-31a0-41ba-bad4-cc47fb9299a4.png)


Data display and analysis: Data Studio.

Link: "Coming Soon".


 ![images-removebg-preview](https://user-images.githubusercontent.com/76137086/174942117-e71f2707-54ac-4c9d-996d-7fddb1b1f1c4.png)


The ETL process was done through Apache Spark, but specifically with PySpark, and other awesome Python tools.

Note:

1 - "/home/usr/abc"
Description: Location where raw data will be allocated.

2 - "/home/usr/def"
Description: Location where the data will remain, until the end of processing.

3 - "/home/usr/ghi"
Description: Location where the data, already processed, will be allocated.

4 - Inside the Etl folder there are two scripts, the directories are different, but follow the same concept, that is, modify according to your use.

5 -  Code Formatter: Yapf.

6 -  For automation processes, the "cron" task scheduler can be used, in the case of Linux distributions.


![png-transparent-ubuntu-server-edition-long-term-support-installation-linux-linux-lamp-linux-ubuntu-16-removebg-preview](https://user-images.githubusercontent.com/76137086/175204618-59d2eb0b-4973-403e-9549-2956eaeaa177.png)


Hardware Settings:

1 - 2 CPU Cores.

2 - 2 Gb Ram.

3 - 10 Gb HD.

4 - OS: Ubuntu Server 22.04.

Note: Hyper-V was used for this project, acting as a hypervisor, building a cluster with 2 nodes, the configuration above is equivalent to a node.


![images-removeb2)](https://user-images.githubusercontent.com/76137086/174941919-db3bd0a0-cc4b-44d1-8f09-66e1b1d0b325.png)


"Project for academic purposes, using Google and Fiocruz databases, to verify the relationship between the mobility of the Brazilian population, and the variation of cases and deaths".

Data providers and maintainers:

https://bigdata-covid19.icict.fiocruz.br/

SIVEP-Gripe.

eSUS-VE.

Google LLC "Google COVID-19 Community Mobility Reports".
https://www.google.com/covid19/mobility/ Accessed: <date>.