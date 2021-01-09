# Zalora_Assignment
#Interview Assignment of Zalora

# INTRODUCTION

TASK -   Spot as many successful trades for bitcoin dataset 

Architecture - 

google storage - for the storage where file will be kept
cloud function/dataproc cluster

to automate we can use cloud function which will do mapping of data or we can go with
dataproc cluster 

BigQuery -  loading the final output to the bigquey table.


Approach -  Downloading the file through url APIm then reading the file into pandas dataframe
			maping  and transforming the data on the basis of constraints
			filter the column which is required and load into biquery table
			
attaching sample csv file ',' delimited - used for testing the python script

python script file name - ETL_Script.py

Conf file Name - config.conf ( It is used for used to configure the parameters and initial settings such as credential for database connectivity and 
								target table names
								url
								)
								
For password sharing encryption is used for safety purpose



Installation Requirements - pandas,pandas_gbq,cryptography,Google cloud connector-python,Bigquery

command - pip install pandas
		  pip install pandas_gbq
		  pip install cryptography
		  pip install google-cloud
		  pip install google-cloud-bigquery

