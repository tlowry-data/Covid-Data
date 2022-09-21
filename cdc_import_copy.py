import pandas as pd
import psycopg2
import numpy as np
import json
import urllib3
import certifi
import requests
import re
from tabulate import tabulate
from datetime import datetime
from datetime import date
import bokeh
import os
import shutil 
import pygsheets
from timeit import default_timer
# use this for password security: https://medium.com/@apoor/quickly-load-csvs-into-postgresql-using-python-and-pandas-9101c274a92f
##IMPORT DATA FROM WEBSITE. ONCE COMPLETE COMMENT CODE TO PREVENT RELOADING THE DATA
try:
      beginning = default_timer()
      timestarted = print(datetime.today())
      with open('time_log.txt','w') as script:
          script.write(timestarted)
      
      url = "https://data.cdc.gov/api/views/n8mc-b4w4/rows.csv"
      http = urllib3.PoolManager(ca_certs=certifi.where())

      #req = requests.get(url, allow_redirects=True)
      with requests.get(url, allow_redirects=True, stream = True) as req:
            with open('COVID-19_Case_Surveillance_Public_Use_Data_with_Geography.csv', 'wb') as f:
              for chunk in req.iter_content(chunk_size = 2048):
                if chunk:
                  f.write(chunk)

      ##MOVE rows.csv FROM PYTHON FOLDER TO SURVEILLANCE FOLDER
      shutil.copy('COVID-19_Case_Surveillance_Public_Use_Data_with_Geography.csv', 'latest_dataset_geo'+str(date.today()) +'.csv')
      shutil.copy('COVID-19_Case_Surveillance_Public_Use_Data_with_Geography.csv', 'db_import.csv')
      ##DELETE OLD FILE FROM PYTHON FOLDER
      os.remove('COVID-19_Case_Surveillance_Public_Use_Data_with_Geography.csv')
      ##SET PARAMETERS OF COLUMNS FOR ALL DATA FILES JUST IN CASE
      pd.options.display.max_columns = None

      with open(r"db_import.csv", "rb") as fp:
          for count, line in enumerate(fp):
                pass
      rows = (count + 1)
      if rows > 2000000:
          df1 = pd.read_csv('db_import.csv',nrows = 20000000, low_memory= False)
          df2 = pd.read_csv('db_import.csv',skiprows = range(1,20000000), nrows = 20000000,low_memory= False)
          df3 = pd.read_csv('db_import.csv',skiprows = range(1,40000000),nrows = 20000000,low_memory= False)
          df4 = pd.read_csv('db_import.csv',skiprows = range(1,60000000),nrows = 20000000,low_memory= False)
          
          new_df= pd.concat([df1,df2,df3,df4])
          
      else:
          df_log = "File is less than 20000000 rows"
          new_df = pd.read_csv('db_import.csv')
      del [df1, df2, df3, df4]
      
      #Check datatypes
      #datatypes = df.dtypes
      #ending = default_timer()
      #time_complete = (ending - beginning)
except (Exception, psycopg2.Error) as error:
          ending = default_timer()
          time_complete = (ending - beginning)
          error_log = "Error while downloading and importing to file.        \n\
                    Total Time for download: "+ str(time_complete) + "seconds \n\
                    Total Rows in File: " +str(rows)
                      #"Data Types: "+datatypes;\
                                
          with open('error_log.txt','w') as error:
           error.write(error_log) 
                      

finally:
       
     
      cvdata_hosp = new_df.query('hosp_yn == "Yes"')
      cvdata_death = new_df.query('death_yn == "Yes"')
      
      def data_agg(x):
         names = {
               'total_ct':x['hosp_yn'].count()
               }
         return pd.Series(names, index=['total_ct'])
     
      cvdata_hosp= cvdata_hosp.groupby(['case_month','res_state','res_county', 'current_status','symptom_status','sex','age_group', 'race', 'ethnicity','hosp_yn','icu_yn', 'death_yn','underlying_conditions_yn'], as_index = False).apply(data_agg)
      
      cvdata_death= cvdata_death.groupby(['case_month','res_state','res_county', 'current_status','symptom_status','sex','age_group', 'race', 'ethnicity','hosp_yn','icu_yn', 'death_yn','underlying_conditions_yn'], as_index = False).apply(data_agg)
      
      cvdata_hosp= cvdata_hosp.sort_values(by = 'res_state')
      
      #Convert to Date Format
      cvdata_hosp['case_month2'] = cvdata_hosp['case_month'].apply(lambda x: datetime.strptime(x, '%Y-%m'))
      
      #Convert to Month-Year
      cvdata_hosp['case_month2'] = cvdata_hosp['case_month2'].dt.to_period('M')
      #Strip Out Year
      cvdata_hosp['case_year2']= pd.Series(cvdata_hosp['case_month2']).dt.year
      
      
      #Authorize Googlesheets
      gc = pygsheets.authorize(service_file = 'cdc-analysis-hosp321-87adbcb8f164.json' )
      #Open Googlesheet
      #sh = gc.open_by_key('1v-wFHdfESfTJIn7MoNNh_ZgGJpIFrmDTHmcAy0EKT3k')
      sh = gc.open('cdc_analysis')
      wks = sh[0]
      wks.clear('*,*')
      cvdata_hosp1 = cvdata_hosp.iloc[:80000,:]
      cvdata_hosp2 = cvdata_hosp.iloc[80001:,:]
      wks.set_dataframe(cvdata_hosp1,(1,1))
      wks.set_dataframe(cvdata_hosp2,(80001,1),extend = True)
      
      ending = default_timer()
      time_complete = (ending - beginning)/60
      timeended = print(datetime.today())
      script_log = "Script Started at: " + timestarted + "\n\
                    Total Time for download and analysis: " + str(time_complete) + " minutes \n\
                    Total Rows in File: " +str(rows) + "\n\
                    Total Rows in Final DataFrame: " + str(len(new_df)) + "\n\
                    Script Ended at: "+ timeended
                
                    
            
                    #"Data Types: "+datatypes;\
      with open('script_log.txt','w') as script:
          script.write(script_log) 
      del[cvdata_hosp,cvdata_hosp1,cvdata_hosp2,cvdata_death]
