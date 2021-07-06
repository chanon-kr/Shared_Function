# py_topping, a topping on some already great libraries
## I create this library to simplify and standardize my friends' projects
This library focus on "simplify" (& lazy) not performance <br><br>

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
This library will *NOT auto install dependencies for you but you could see the list of dependencies below<br><br>
You could see samples of how to use this library inside the samples folder in github<br>
(https://github.com/chanon-kr/Shared_Function)
<br>

***
### database
  - Original to Work with SQL Server, MySQL, PostGreSQL and SQLite
  - Since 0.3.5, It can work with GCP's BigQuery, still need more test on Cloud
  - To read view, table or store procedure as pandas dataframe 
  - To insert pandas dataframe into SQL with option for existing row :
    - Replace same key(s) with new from dataframe with option to use ">" or "<" condition
    - Add Only row with non-existing key(s)
    - Replace whole table
  - Can parallel insert by using dask's delayed (not recommend)
  - Can't read Store Procedure in PostGreSQL will solve this in later version
  - Will working with Oracle Database in later version
  - Dependencies for this library as listed ;<br>
```sqlalchemy```
```pandas```
```dask```
```toolz```
  - Additional dependencies for GCP's BigQuery as listed ;<br>
```pybigquery```
```pandas-gbq```
```google-cloud-bigquery```
```google-cloud-bigquery-storage```
 - Sample of use => https://github.com/chanon-kr/Shared_Function/blob/main/samples/database.ipynb

***
### sharepoint
  - to download file from SP365 or SP on prim
  - to read csv/excel from SP365 as pandas dataframe
  - to download List as csv or pandas dataframe from SP365
  - upload file to SP365 or SP on prim
  - Dependencies for this library as listed ;
```Office365-REST-Python-Client (recommend 2.2.1)```
  - Sample of use => https://github.com/chanon-kr/Shared_Function/blob/main/samples/sharepoint.ipynb

***
### gcp
  - to download file from GCP's bucket Storage
  - upload file from GCP's bucket Storage
  - Dependencies for this library as listed ;
```...```
  - Sample of use => https://github.com/chanon-kr/Shared_Function/blob/main/samples/gcp.ipynb

***
### data_preparation
  - Encode categorical column
  - Create lagging parameter
  - Simple Deep Learning Model for Regression
  - Dependencies for this library as listed ;
```pandas```
```sklearn```
```tensorflow>=2```
  - Sample of use  
    - Data Prep => https://github.com/chanon-kr/Shared_Function/blob/main/samples/data_preparation.ipynb
    - Simple Deep Learning => https://github.com/chanon-kr/Shared_Function/blob/main/samples/lazy_ml.ipynb

***
### general_use
  - To send email with python 
  - To logging in csv file
  - To check port status
  - To send LINE message, sticker or picture with line notify
  - Sample of use 
    - LINE => https://github.com/chanon-kr/Shared_Function/blob/main/samples/lazy_LINE.ipynb
    - EMAIL => https://github.com/chanon-kr/Shared_Function/blob/main/samples/email_sender.ipynb
    - Other => https://github.com/chanon-kr/Shared_Function/blob/main/samples/other_function.ipynb

***
### run_pipeline
  - to run your python or notebook scripts 
    - with logging as a csv
    - with logging into sql table
    - with emailing the log when have/don't have errors
    - Dependencies for this library as listed ;
```papermill```
  - Sample of use  => https://github.com/chanon-kr/Shared_Function/blob/main/samples/run_pipeline.ipynb