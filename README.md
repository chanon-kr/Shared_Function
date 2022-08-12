<img src="https://user-images.githubusercontent.com/64777509/179344226-e9440447-a69d-46c1-a535-d18c1680e73d.jpg" align="center" width="228">

# py_topping, a topping on great libraries
## This library focus on "simplify" (& laziness :P) <br><br>

PyPi Project Page : https://pypi.org/project/py-topping
<br><br>To Install the Stable Version<br>
```python
pip install py-topping
```
<br>To Install the Newest Version<br>
```python
pip install git+https://github.com/chanon-kr/Shared_Function.git
```
<br>
This library will *NOT auto install dependencies for you but you could see the list of dependencies in sample links<br><br>
You could see samples of how to use this library inside the samples folder in github<br>
https://github.com/chanon-kr/Shared_Function
<br>

***
### database
  - To Work with SQL Server, MySQL, PostGreSQL, SQLite and Google BigQuery
  - To read view, table or store procedure as pandas dataframe 
  - To insert pandas dataframe into SQL with different methods
    - from 0.3.18, will roll back if job fail, except Google BigQuery
  - Can't read Store Procedure in PostGreSQL will solve this in later version
  - Will working with Oracle Database in later version
  - Dependecies and Sample of use => https://github.com/chanon-kr/Shared_Function/blob/main/samples/database.ipynb

***
### sharepoint
  - to download file from SP365 or SP on prim
  - to read csv/excel from SP365 as pandas dataframe
  - to download List as csv or pandas dataframe from SP365
  - upload file to SP365 or SP on prim
  - Dependecies and Sample of use => https://github.com/chanon-kr/Shared_Function/blob/main/samples/sharepoint.ipynb

***
### gcp
  - to download and upload file from GCP's bucket Storage
  - Dependecies and Sample of use => https://github.com/chanon-kr/Shared_Function/blob/main/samples/gcp.ipynb

***
### socket
  - To receive and send data though socket (TCP/IP)
  - Dependecies and Sample of use => https://github.com/chanon-kr/Shared_Function/blob/main/samples/lazy_TCP.ipynb
***
### data_preparation
  - Encode categorical column
  - Create lagging parameter
  - Simple Deep Learning Model for Regression
  - Dependecies and Sample of use 
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
  - Dependecies and Sample of use
    - LINE => https://github.com/chanon-kr/Shared_Function/blob/main/samples/lazy_LINE.ipynb
    - EMAIL => https://github.com/chanon-kr/Shared_Function/blob/main/samples/email_sender.ipynb
    - Other => https://github.com/chanon-kr/Shared_Function/blob/main/samples/other_function.ipynb

***
### run_pipeline
  - to run your python or notebook scripts 
  - to create FastAPI
  - Dependecies and Sample of use  
    - run pipeline=> https://github.com/chanon-kr/Shared_Function/blob/main/samples/run_pipeline.ipynb
    - create FastAPI => https://github.com/chanon-kr/Shared_Function/blob/main/samples/api.ipynb
