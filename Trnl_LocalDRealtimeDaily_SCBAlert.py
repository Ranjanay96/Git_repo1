import pandas as pd 
import datetime
import numpy as np
#import matplotlib.pyplot as plt
#from pyspark.sql import SparkSession
#from pyspark.sql.types import *
#import pyspark.sql.functions as sf
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
print("NY time:", datetime_Now.strftime('%Y-%m-%d %H:%M:%S.%f'))
before_time=(datetime.now(tz_india)-timedelta(minutes=15)) # change time detla according to frequency of alert
before_time.strftime('%Y-%m-%d %H:%M:%S.%f')

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
#mystr = "2022-08-16-"
mystr = mystr.replace('-','/')
mystr
df_final = pd.DataFrame()

tz_india = pytz.timezone('Asia/Kolkata') 
datetime_Now = datetime.now(tz_india)
print("IST time:", datetime_Now.strftime('%Y-%m-%d %H:%M:%S.%f'))
before_time=(datetime.now(tz_india)-timedelta(minutes=15)) # change time detla according to frequency of alert
before_time.strftime('%Y-%m-%d %H:%M:%S.%f')
path_len=len(r'"C:\Users\Piyush\LocalDTrnl\TrnlAzureLocalD\'')
print(path_len)
path = r'"C:\Users\Piyush\LocalDTrnl\TrnlAzureLocalD\''+ str(todaydate) + r'\"'
print(path,len(path))
path_new=path[1:path_len-1]+path[(path_len):len(path)-1]
#path_new
print(path_new,len(path_new))
all_files = glob.glob(path_new+"*.csv")
print(path_new+"*.csv")
print(all_files)
df=pd.DataFrame()
for i in all_files:
    print(i)
    df_i=pd.read_csv(i)
    df=df.append(df_i)
df.shape
df_final = df_final.append(df)
print(df_final.shape)
print(df.shape)
df_final.head(4)
#print(df_final[df_final["itemname"] == "MCR..WMS_Pyranometer_GII"].tail(1)["timestamp"])
print(df_final[df_final["itemname"] == "ICR_12..Inv_02_PV_24_Current"])

df_final['deviceid'].unique()

df_final.head()

df = df_final.copy()

df.head()

len(df["itemname"].unique())

df.head(3)
df_final.tail()

df.tail()
df["deviceid"].unique()

from datetime import datetime
import pytz
df_scb=df.copy()
df.drop(columns=["deviceid","quality","timestamp","EventProcessedUtcTime","PartitionId","EventEnqueuedUtcTime","IoTHub"],inplace=True)
df.rename(columns = {"ISTtime": "timestamp"},inplace = True)

df.sort_values(by="timestamp",ascending=True,inplace=True)
df.sort_values(by="timestamp",ascending=True,inplace=True)
df["timestamp"]=df["timestamp"].astype("datetime64[ns]")
df["Date"]=pd.DatetimeIndex(df["timestamp"]).date
df["Date"]=df["Date"].astype("datetime64[ns]")
df["Time"]=pd.DatetimeIndex(df["timestamp"]).time
df["Hour"]=pd.DatetimeIndex(df["timestamp"]).hour
df["Minute"]=pd.DatetimeIndex(df["timestamp"]).minute
df["value"]=df["value"].astype("float64")
print(df.dtypes)

print(df.shape)
print(df[df["itemname"] == "ICR_12..Inv_02_PV_24_Current"].tail(1)["timestamp"])
df.iloc[-1,:]
Radiation_Block1=df[(df['itemname']=="MCR..WMS_Pyranometer_GII") & (df["Date"]==str(df["Date"].mode()[0]).split(" ")[0]) &  (df['value']>=2) & (df["Hour"]>=6) & (df["Hour"]<20)]
Radiation_Block1=Radiation_Block1[["timestamp","itemname","value"]]
Radiation_Block1["timestamp"]=Radiation_Block1["timestamp"].astype("datetime64[ns]")
Radiation_Block1["Hour"]=pd.DatetimeIndex(Radiation_Block1["timestamp"]).hour
Radiation_Block1["Minute"]=pd.DatetimeIndex(Radiation_Block1["timestamp"]).minute
Data_To=str(Radiation_Block1["timestamp"].head(1).iloc[0])
print("Data To",Data_To)
Data_From=str(Radiation_Block1["timestamp"].tail(1).iloc[0])
print("Data From",Data_From)
df=df[(df["timestamp"]>=Data_To) & (df["timestamp"]<=Data_From)]
df=df.drop_duplicates()
df["value"]=df["value"].astype("float32")
Up_Hour=df[(df["itemname"]=="MCR..WMS_Pyranometer_GII") & (df["value"]>=2) ].head(1)["Hour"].iloc[0]
Sleep_Hour=df[(df["itemname"]=="MCR..WMS_Pyranometer_GII") & (df["value"]>=2) ].tail(1)["Hour"].iloc[0]
Up_Minute=df[(df["itemname"]=="MCR..WMS_Pyranometer_GII") & (df["value"]>=2) ].head(1)["Minute"].iloc[0]
Sleep_Minute=df[(df["itemname"]=="MCR..WMS_Pyranometer_GII") & (df["value"]>=2) ].tail(1)["Minute"].iloc[0]
current_time=datetime_Now.strftime('%Y-%m-%d %H:%M:%S.%f')
print(current_time)
before_time=before_time.strftime('%Y-%m-%d %H:%M:%S.%f')
print(before_time)
five_min_before_time=five_min_before_time=(datetime.now(tz_india)-timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S.%f')
df=df[(df["timestamp"]<=str(current_time)) & (df["timestamp"]>=str(five_min_before_time))]
df.tail()
df.head()


df_scb=df.copy()
df1=df.copy()
df2=df.copy()
scb_static_file=pd.read_csv("Trnl_SMB.csv")
scb_static_file=list(scb_static_file["tagname"])[0:len(scb_static_file)]

print(len(scb_static_file))

df_scb=df_scb[df_scb["itemname"].isin(scb_static_file)]
print(len(df_scb["itemname"].unique()))
presentTags = list(df_scb["itemname"].unique())
missingTags = [i for i in scb_static_file if i not in presentTags]
print("TotalMissingTags",missingTags)
print(len(missingTags))
all_scb=df_scb.groupby("itemname")["value"].agg("sum").reset_index()
all_faulty_scb=all_scb[all_scb["value"]==0]
faulty_scb_list=all_faulty_scb["itemname"].to_list()
faulty_scb_list

static_trnl_scb=pd.read_csv("Trnl_SMB.csv")
static_trnl_scb

faulty_scb_list=static_trnl_scb[static_trnl_scb["tagname"].isin(faulty_scb_list)]["SCBNameinField"].to_list()
print("FaultyList",faulty_scb_list)

columns=["Tagname"]
df_scb=pd.DataFrame(faulty_scb_list,columns=columns)
df_scb.to_excel("C:\\Users\\Piyush\\OneDrive\\LocalD\\Trnl\\TrnlSCB_Alerts.xlsx",index=False)

Radiation_Block1 = Radiation_Block1[(Radiation_Block1["timestamp"]<=str(current_time)) & (Radiation_Block1["timestamp"]>=str(five_min_before_time))]
Radiation_max = Radiation_Block1['value'].max()

Radiation_sum = Radiation_Block1['value'].sum()


print(Radiation_Block1)



if (len(all_faulty_scb) > 0) and (Radiation_max > 50):
    md1 = {'Date':str(todaydate),'SCB':faulty_scb_list,'From':str(before_time),'To':(current_time),'Radiation_Sum':Radiation_sum}
    dfa = pd.DataFrame(md1)
    db = pd.read_excel('C:\\Users\\Piyush\\OneDrive\\LocalD\\Trnl\\TrnlFaultySCBsDB_Loss.xlsx')
    db = db.append(dfa)
    db.to_excel('C:\\Users\\Piyush\\OneDrive\\LocalD\\Trnl\\TrnlFaultySCBsDB_Loss.xlsx',index = False)
    import pymsteams
    TrnlFaultySCBs = ''
    for i in faulty_scb_list:
        TrnlFaultySCBs+=i+'\n\n'
    TrnlFaultySCBs
    x="**Faulty SCB(s):**"+"\r\n\r"+(TrnlFaultySCBs[:-2])
    myTeamsMessage = pymsteams.connectorcard("https://ayanapower.webhook.office.com/webhookb2/0472b42e-7f66-444d-a4b0-99f6157238dd@59b60474-e282-44b5-881c-bb9ce815690c/IncomingWebhook/d76fb77a3f154af180edee6009f8247e/0a64ab19-2984-4746-9447-21882231bb32")
    myTeamsMessage.text(str(x))  
    myTeamsMessage.send()

print(len(faulty_scb_list))