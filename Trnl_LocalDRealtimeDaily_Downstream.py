   ##################################################################################################################################
    
                                               ####################            BHADLA              ###################
                                        
                                        
#### This script runs every 10m to bring the data from Azure Cloud in Local Drive of this VM                                   
                                      
                                        
import pandas as pd 
import datetime
import numpy as np
from datetime import datetime 
from datetime import date 
from datetime import timedelta
# from pyspark.sql.functions import to_date
from datetime import time
import pytz
from pathlib import Path
import pymsteams
import glob
import warnings
warnings.filterwarnings("ignore")

from datetime import datetime
import pytz

import subprocess

from datetime import datetime
import pytz

tz_india = pytz.timezone('Asia/Kolkata') 
datetime_Now = datetime.now(tz_india)
print("IST_Current:", datetime_Now.strftime('%Y-%m-%d %H:%M:%S.%f'))
before_time=(datetime.now(tz_india)-timedelta(minutes=20)) # change time detla according to frequency of alert
before_time.strftime('%Y-%m-%d %H:%M:%S.%f')
print("IST_Before: ",before_time)

LoopCounter=1
tz = pytz.timezone('Asia/Kolkata')
todaydate = datetime.date(datetime.now(tz))
print(todaydate)
mintime = datetime.min.time()
DateList =[]
while LoopCounter <= 1 :
    DateList.append( datetime.date ( (datetime.combine(todaydate, mintime) - timedelta(days=LoopCounter)) ))
    LoopCounter += 1
DateListLength = len(DateList)
print(DateList)
TableLoopCounter = 0
DateLoopCounter = 0

mystr = str(todaydate) + "-"
mystr = mystr.replace('-','/')
mystr

testing = r'cmd /k Azcopy copy "https://ayanadatalake.blob.core.windows.net/trnl/TRNL_Realtime/' + mystr + r'*?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-01-01T15:37:16Z&st=2022-08-17T07:37:16Z&spr=https&sig=yxvlbw1EnST1yQz4bFlFH5iQj32E6WArE2SUOXpUGvo%3D"'+ " " + r'"D:\OneDrive - Ayana Renewable Power private Limited\Desktop\LocalDTrnl\TrnlAzureLocalD'+ str('\\') + str(todaydate) + r'"' +" " + r"--recursive"
testing

import subprocess
process = subprocess.Popen(testing,shell=True ,stdin=subprocess.PIPE,stdout=subprocess.PIPE,cwd=r'D:\OneDrive - Ayana Renewable Power private Limited\Desktop\LocalDTrnl\azcopy_windows_amd64_10.16.0',stderr=subprocess.PIPE)
o,e=process.communicate()
print(o)           