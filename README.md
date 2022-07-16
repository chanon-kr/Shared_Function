<img src="https://user-images.githubusercontent.com/64777509/179343808-7d04effe-d9bd-446c-bf79-8abf7b56540d.jpg" align="center" width="228">

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
This library will *NOT auto install dependencies for you but you could see the list of dependencies in sample links<br><br>
You could see samples of how to use this library inside the samples folder in github<br>
(https://github.com/chanon-kr/Shared_Function)
<br>

***
### database
  - To Work with SQL Server, MySQL, PostGreSQL, SQLite and Google BigQuery
  - To read view, table or store procedure as pandas dataframe 
  - To insert pandas dataframe into SQL with different methods
    - from 0.3.18, will roll back if job fail, except Google BigQuery
  - Can't read Store Procedure in PostGreSQL will solve this in later version
  - Will working with Oracle Database in later version
  - Denpendecies and Sample of use => https://github.com/chanon-kr/Shared_Function/blob/main/samples/database.ipynb

***
### sharepoint
  - to download file from SP365 or SP on prim
  - to read csv/excel from SP365 as pandas dataframe
  - to download List as csv or pandas dataframe from SP365
  - upload file to SP365 or SP on prim
  - Denpendecies and Sample of use => https://github.com/chanon-kr/Shared_Function/blob/main/samples/sharepoint.ipynb

***
### gcp
  - to download and upload file from GCP's bucket Storage
  - Denpendecies and Sample of use => https://github.com/chanon-kr/Shared_Function/blob/main/samples/gcp.ipynb

***
### data_preparation
  - Encode categorical column
  - Create lagging parameter
  - Simple Deep Learning Model for Regression
  - Denpendecies and Sample of use 
    - Data Prep => https://github.com/chanon-kr/Shared_Function/blob/main/samples/data_preparation.ipynb
    - Simple Deep Learning => https://github.com/chanon-kr/Shared_Function/blob/main/samples/lazy_ml.ipynb

***
### general_use
  - To send email with python 
  - To logging in csv file
  - To check port status
  - To send LINE message, sticker or picture with line notify
  - To Create diff hour of (Desired UTC) - (Environment UTC)
  - To Check health of your machine
  - Denpendecies and Sample of use
    - LINE => https://github.com/chanon-kr/Shared_Function/blob/main/samples/lazy_LINE.ipynb
    - EMAIL => https://github.com/chanon-kr/Shared_Function/blob/main/samples/email_sender.ipynb
    - Other => https://github.com/chanon-kr/Shared_Function/blob/main/samples/other_function.ipynb

***
### run_pipeline
  - to run your python or notebook scripts 
  - to create FastAPI
  - Denpendecies and Sample of use  
    - run pipeline=> https://github.com/chanon-kr/Shared_Function/blob/main/samples/run_pipeline.ipynb
    - create FastAPI => https://github.com/chanon-kr/Shared_Function/blob/main/samples/api.ipynb
