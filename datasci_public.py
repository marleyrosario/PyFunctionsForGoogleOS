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
    Step 19: Grant approval to at the very minimum data editor and reader, for the purposes of this project. Giving yourself
    admin priviledges doesn't hurt either. 
    Step 20:


"""
"""

"""

