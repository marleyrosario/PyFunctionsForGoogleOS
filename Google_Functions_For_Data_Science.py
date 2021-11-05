# -*- coding: utf-8 -*-
"""
Created on Thu Nov  4 16:36:23 2021

@author: @marleyrosario
"""

"""
Hey there! Here are some cool functions to help you connect to BigQuery and 
Google Sheets and work with pandas. 

I have read a bunch of articles but many of them don't work or don't provide enough 
context to work with in a quick and  easy fashion. These functions work really 
well with Google Workspace accounts or normal Google accounts. 
Use them for your ETLs, ELTs, ML models to help build bring in your data into a structured format 
for an object like a dataframe. Google has some of the best tools for brute data collection, 
and if you want to analyze data using Google's free and open-sourced platforms, 
then these functions work well for you. 

"""
"""
Section 0: Package installation
"""

"""
Start by pip installing the following packages:
    
https://pypi.org/project/google-auth/
https://pypi.org/project/google-cloud/
https://pypi.org/project/gspread/
https://pypi.org/project/oauth2client/

"""    
"""
Now we import!!
"""    
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import bigquery_storage
from oauth2client.service_account import ServiceAccountCredentials

"""
Let's bring in our stats and datascience packages
"""

import pandas as pd
import numpy as np
from datetime import date
from oauth2client.service_account import ServiceAccountCredentials

"""
Section 1: Connect to BigQuery and bring in as a dataframe
"""

"""
After making sure that you can import all these packages into your active conda
environment, the first step is to enable an active Google Cloud Platform project.
The steps vary depending on what you are trying to accomplish in python. 

Case 1 --> Engingeering Ops and Containers 
If your project requires multiple environments or is working with containizeration, 
then I would download the SDK and start from the desktop software immediately. 

Additionally projects involving python frameworks like DJango, accessing spacial data,
or any cloud project beyond BigQuery or Sheets, I would also strongly recommend starting from the SDK. 

Here are the docs to launch this process:
https://cloud.google.com/appengine/docs/standard/nodejs/building-app/creating-project

I would also alert that in order to do this case, you need to be somewhat familiar with the terminal language.

In a future script, I can share terminal code that you can use in Google's PowerShell or the command prompt
of whatever package manager you are using. 

Section 1a: Setting up GCP, BQ and an IAM Service Account 

Case 2 --> Data Science and complex Analysis 
For all the projects strictly using BigQuery or Sheets, often specifically for ETLs, ELTs, or 
more complex analysis projects, go to this link: https://console.cloud.google.com/

Below are the steps to help guide this process:
    Step 1: Click Sign up for free (you will need to put in your payment information, but unless you
                                    start a virtual-machine you won't ever get billed')
    Step 2: After signing up Click on 'Console' in the upper right hand corner 
    Step 3: On the side bar on the left hand side, scroll all the way to "Big Data" to the "BigQuery" tab
    Step 4: Scroll on to the "BigQuery" tab and open the "SQL Workspace"
    Step 5: There you will see the technical name for your project. Often times, its two random names 
    concatenated with an emdash like "nonsense-simpson" and then an emdash and then a 6 digit number. A full example
    would be "nonsense-simpson-111222". 
    Step 6: Create a dataset 
    Step 7: Once you've given your dataset a title, it will appear under your first pinned project, in the explorer
    of the SQL Workspace. 
    Step 8: From there, click on the dataset and then create a table.
    Step 9: Creating a table is a bit tricky. What file you are uploading will change how the process goes about
    and what not. In future files, I can explain the differences, but for the purposes of this blog, we will start with a 
    csv.
    Step 10: Click upload and then select a file. NOTE: In the columns of the csv (keys in a dictionary or AVRO file but again not for this project), 
    make sure there are no weird headers. Things that could make BQ return an error would be columns with non-alphanumeric characters. 
    Step 11: Give the table a name, again no non-alphanumeric characters
    Step 12: Generally, I go to Advanced Options and set the number of errors equal to number of rows of the file I am trying to upload. 
    This will make sure BigQuery will allow the table to be saved in order to be uploaded in the first place. If there is a type error (an error that basically says
    the data type for a cell is incorrect relative to the rest of the column), Big Query will return it to us on a general query. 
    Step 13: Now, you should see the table appear on the left hand side directly underneath the dataset. 
    Step 14: Once you have done this, I generally would do just a general query which is formatted like "Select * From 'project_id.dataset_id.table_name'" just to see
    if my data will query with no problem. If you click on the large "QUERY" button (it depends on whether you are using legacy or the newer UI of the SQL Workspace for where
    this button can be found), a general query will come up. 
    Step 15: Now that you have set up your table and everything in Big Query you need to create an API JSON token that you can use 
    to programmatically connect to the database. 
    Step 16: Go back to the Google Cloud Platform side bar on the left hand side, and under Products click
    IAM Admin and then click "Service Accounts"
    Step 17: Click Create Service Accounts
    Step 18: Give your service account a name. Note: You will have to do two of these - 1 for BQ and 1 for sheets. 
    Step 19: Grant approval to at the very minimum data editor and reader, for the purposes of this project giving yourself
    admin priviledges doesn't hurt either. 
    Step 20: Create the service account. 
    Step 21: From here your service account will appear kind of looking like an email in a list of service accounts. Click on the service account you just made
    Step 22: Go to Keys and click ADD KEY
    Step 23: Select JSON, and now a JSON key should have been downloaded to your computer. 
    Step 24: Now to start programming. 

Section 1b: Defining your environment 

Step 1: Define your environment and set a path to the IAM Service account. 

"""
def choose_user(path, fname):
    import os
    user = os.path.join(path, fname)
    return user 

user = choose_user(path = "PATH", fname = "fname")

# Path is the folder in where your JSON key downloaded to on your computer. 
# FName is the name of the JSON key. On Windows file explorer, you can for sure click on the folder and then 
# copy path

"""
Step 2: Creating a Dataframe from BigQuery
"""
def create_data_frame(path, query_string, project_id='PROJECT_ID'):
    credentials = service_account.Credentials.from_service_account_file(path)
    client = bigquery.Client(credentials= credentials,project=project_id)
    bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
    query_string = "Select {query_string} from {table_id}".format(query_string, table_id)
    dataframe = (client.query(query_string).result().to_dataframe(bqstorage_client=bqstorageclient))
    return dataframe

df = create_data_frame(path = user, query_string = query_string, table_id, project_id = 'PROJECT_ID')

"""
So this function will take any query string that you want in the select statement 
and any table id from your bigquery dataset and then subsequently uploads it to your environment as a 
dataframe. This function works for Select statements only. Any DML SQL language queries will not work. 
"""
"""
Now let's say you wanted to extract this dataset and add a column and then send the resulting dataframe to Big Query.

A common purpose for this would be to do BigData batch operations or for ETLs where your database is BigQuery.

Here's a cool function you can use to send data back to BigQuery from a dataframe.

"""

df['New_Column'] = "Random Data"

def send_data_to_bq(path, dataframe, table_id, field_name, project_id ='PROJECT_ID'):
    # establishing authentication
    credentials = service_account.Credentials.from_service_account_file(path)
    client = bigquery.Client(credentials= credentials,project=project_id)
    job_config = bigquery.LoadJobConfig(schema = [bigquery.SchemaField(field_name, 
    bigquery.enums.SqlTypeNames.STRING)],write_disposition = "WRITE_TRUNCATE")
    #loading the data from BQ table to pandas dataframe with all the parameters set to your variables
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    # telling python to map the client name equal to the code name based on dictionary key:values
    job.result()
     # making an API request.
    table = client.get_table(table_id)

send_data_to_bq(path = user, df =  df, field_name = "field_name", table_id = "table_id", project_id= 'PROJECT_ID')

"""
So this function is pretty strong in my opinion and has a lot of different applications. 

Set your new dataframe into the function and then declare a field name for BQ to understand where 
to start reading the schema (column headers in datasci - speak). 

Make sure that the field you set is a String value. On line 171 using the enums part of the bigquery
package, I am sure you could change it to be an INT value but for sure FLOAT's and BOOL's do not work. 

From there just load up the table_id and you should be perfectly fine and see the dataset in Bigquery under the
table you made. 

The table name can be new or you can overwrite the one you made previously. If the dataset and project_id have 
a typo or are something different then it will return a table not found error.
"""

"""
Section 2: Creating a dataframe from Google Sheets
    - Step 1: Create a new IAM Account. You can follow the steps above and replicate it for a google sheet. 
"""
def choose_user_sheets(path, fname):
    import os
    user = os.path.join(path, fname)
    return user 

sheet_user = choose_user_sheets(path = "PATH", fname = "fname")

def google_sheet_to_dataframe(user, sheet_name="Sheet_Name", worksheet="Work_Sheet_Name"):
    scope=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/spreadsheets" , "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds=ServiceAccountCredentials.from_json_keyfile_name(user_2, scope)
    client=gspread.authorize(creds)
    sheet=client.open(sheet_name).worksheet(worksheet)
    df=pd.DataFrame(sheet.get_all_records())
    return df

df = google_sheet_to_dataframe(user=sheet_user, sheet_name = "Sheet_Name", worksheet = "Work_Sheet_Name")

""" 
You can also read the dataframe with the URL as well instead of declaring a sheet or a worksheet name.
"""
def google_sheet_to_dataframe_url(user, url, sheet):
    scope=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/spreadsheets" , "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds=ServiceAccountCredentials.from_json_keyfile_name(user_2, scope)
    client=gspread.authorize(creds)
    sheet=client.open_by_url(sheet_url).worksheet(sheet)
    df=pd.DataFrame(sheet.get_all_records())
    return df

df = google_sheet_to_dataframe_url(user=sheet_user, url="url", sheet = "Work_Sheet_Name")

"""
The neat thing about python is that you can also do this through a list comprohension that grabs all 
the workbooks in a google sheet at once. This function will return a list of dataframes!!

"""
def links_to_list(sheet_url, sheet_tabs):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(user, scope)
    client = gspread.authorize(creds)

    # open each sheet using the url
    list_of_sheet_dfs = [client.open_by_url(sheet_url).worksheet(sheet)
                for sheet in sheet_tabs]
    list_of_dfs = [pd.DataFrame(sheet.get_all_records())
                        for sheet in list_of_sheet_dfs]
    return list_of_dfs

list_of_dfs = links_to_list(sheet_url = "URL", sheet_tabs = ['Tab One', 'Tab Two', 'Tab Three'])

"""
Now if you're good with a list and can use list comprohensions to mess with the dataframes, then you should 
be chilling from here. If you are more interested in just having one dataframe and then applying or mapping functions 
to columns then here is a quick function to merge pandas dataframes in a list by their commonly shared columns. 
"""
def merge_dfs_in_a_list(list_of_dfs):  
    common_cols = list(set.intersection(*(set(df.columns)
                        for df in list_of_dfs)))
    full_df = pd.concat([df[common_cols]
                                for df in list_of_dfs], ignore_index=True)

    return full_df

full_df = merge_dfs_in_a_list(full_df)


                            






