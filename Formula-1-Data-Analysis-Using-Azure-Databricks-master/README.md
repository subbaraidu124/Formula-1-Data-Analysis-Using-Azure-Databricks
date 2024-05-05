# Formula-1-Data-Analysis-Using-Azure-Databricks

### Introduction
In this project, I have developed a solution architecture for a data engineering solution using Azure Databricks, Azure Data Lake Gen2 and Power BI. The dataset used is Formula 1 historical data from 1950 to 2019. Data is taken from https://ergast.com/mrd/

Databricks is a data analytics platform that uses Apache Spark for data engineering solutions. It is available as service in Microsoft Azure.

### Dataset
Formula 1 Racing data of all the races, drivers, teams, circuits and other data related to races.

### Storage
I have used Azure Data lake Gen2 for storage. Data lake has to be mounted to dbfs in order to access from databricks workspace. 

Steps to connect databricks to Azure data lake:
1) Create storaeg account
2) Create 3 blob containers: raw, processed and presentation
3) Create service principle which can be considered as access to services we wanna use and helps applications to access the resources via service principal.
4) Add a secret.
5) Go to Storage Account -> Access Control -> Add role as data contributor
6) Create key vault to add all the secrets in the vault.
7) Create a notebook in databricks and mount storage account to db workspace.

