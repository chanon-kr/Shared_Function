# py_topping, a topping on some already great libraries
## I create this library to make my team's projects easier and have VCS

PyPi Project Page : (https://pypi.org/project/py-topping)
<br><br>To Install a Stable Version<br>
```python
pip install py-topping
```
<br>To Install a Newest Version<br>
```python
pip install git+https://github.com/chanon-kr/Shared_Function.git
```
<br>
You could see samples of how to use this library inside the samples folder in github<br>
(https://github.com/chanon-kr/Shared_Function)
<br><br>


***
## data_connection
***
### sharepoint
  - to download file from SP365 or SP on prim
  - to read csv/excel from SP365 as pandas dataframe
  - to download List as csv or pandas dataframe from SP365
  - upload file to SP365 or SP on prim
  - Dependencies for this library as listed ;
```Office365-REST-Python-Client (recommend 2.2.1)```
<br><br>
***
### gcp
  - to download file from GCP's bucket Storage
  - upload file from GCP's bucket Storage
  - Dependencies for this library as listed ;
```...```
<br><br>
***
### database
  - Original to Work with SQL Server, MySQL, PostGreSQL and SQLite
  - Can work with GCP's BigQuery, still need more test on Cloud
  - To read view, table or store procedure as pandas dataframe 
  - To insert pandas dataframe into SQL with option for existing row :
    - Replace same key(s) with new from dataframe with option to use ">" or "<" condition
    - Add Only row with non-existing key(s)
    - Replace whole table
  - Can parallel insert by using dask's delayed (not recommend)
  - Can't read Store Procedure in PostGreSQL and BigQuery will solve this in later version
  - Will working with Oracle Database in later version
  - Dependencies for this library as listed ;
```sqlalchemy```
```pandas```
```dask```
```toolz```
  - Additional dependencies for GCP's BigQuery as listed ;
```pybigquery```
```pandas-gbq```
```google-cloud-bigquery```
```google-cloud-bigquery-storage```
<br><br>
***
## data_preparation
***
  - Encode categorical column
  - Create lagging parameter
  - Simple Deep Learning Model for Regression
  - Dependencies for this library as listed ;
```pandas```
```sklearn```
```tensorflow>=2```
<br><br>
***
## general_use
***
### general_use
  - To send email with python
  - To logging in csv file
  - To check port status
  - To send LINE message, sticker or picture with line notify

***
## run_pipeline
***
### run_pipeline
  - to run your python or notebook scripts 
    - with logging as a csv
    - with logging into sql table
    - with emailing the log when have/don't have errors
    - Dependencies for this library as listed ;
```papermill```