# -*- coding: utf-8 -*-
"""
Created on Thu Nov  4 16:36:23 2021

@author: @marleyrosario
"""

"""
Hey there! Here are some cool functions to help you connect to BigQuery and 
Google Sheets and work with pandas. I have read a bunch of articles but many
of them don't work or don't provide enough context to work with in a quick and 
easy fashion. These functions work really well with Google Workspace accounts 
or normal Google accounts. Use them for your ETLs, ELTs, ML models to help 
build bring in your data into thea structured format. Google has some of 
the best tools for brute data collection, and if you want to analyze data using
Google's free and open-sourced platforms, then these functions work well for you. 

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

Case 2 --> 
For all the projects strictly using BigQuery or Sheets, often specifically for ETLs, ELTs, or 
more complex analysis projects, go to this link: https://console.cloud.google.com/

Below are the steps to help guide this process:
    Step 1: Click Sign up for free (you won't need to put in your payment information)
    Step 2:  


 
"""
"""

"""

