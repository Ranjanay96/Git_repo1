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
print("NY time:", datetime_Now.strftime('%Y-%m-%d %H:%M:%S.%f'))
before_time=(datetime.now(tz_india)-timedelta(minutes=12)) # change time detla according to frequency of alert
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
print("Datelist",DateList)
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
before_time=(datetime.now(tz_india)-timedelta(minutes=10)) # change time detla according to frequency of alert
before_time.strftime('%Y-%m-%d %H:%M:%S.%f')
path_len=len(r'"C:\Users\Piyush\LocalDBhdl\BhdlAzureLocalD\'')
print(path_len)
path = r'"C:\Users\Piyush\LocalDBhdl\BhdlAzureLocalD\''+ str(todaydate) + r'\"'
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

# Above Script is to read the Downstream csv in hard disk of this VM 


current_time=datetime_Now.strftime('%Y-%m-%d %H:%M:%S.%f')
print("IST_current",current_time)

before_time=before_time.strftime('%Y-%m-%d %H:%M:%S.%f')
print("IST_before",before_time)

five_min_before_time=five_min_before_time=(datetime.now(tz_india)-timedelta(minutes=8)).strftime('%Y-%m-%d %H:%M:%S.%f') # change time detla according to frequency of alert
#before_time.strftime('%Y-%m-%d %H:%M:%S.%f')

# df=df[(df["ISTtime"]<=str(current_time)) & (df["ISTtime"]>=str(before_time))]
# df.tail(5)

from datetime import datetime
import pytz

tz_india = pytz.timezone('Asia/Kolkata') 
datetime_Now = datetime.now(tz_india)
print("IST time:", datetime_Now.strftime('%Y-%m-%d %H:%M:%S.%f'))
before_time=(datetime.now(tz_india)-timedelta(minutes=30)) # change time detla according to frequency of alert
before_time.strftime('%Y-%m-%d %H:%M:%S.%f')


df_scb=df.copy()
#df = pd.read_parquet(location1)

#spark.sql("drop table phelan.invperformance")

df.drop(columns=["deviceid","quality","timestamp","EventProcessedUtcTime","PartitionId","EventEnqueuedUtcTime","IoTHub"],inplace=True)

#location_input="abfss://repono@ayanadatalake.dfs.core.windows.net/staticfiles/"

df.sort_values(by="ISTtime",ascending=True,inplace=True)
df.sort_values(by="ISTtime",ascending=True,inplace=True)
#df=df[df["sitename"]=="ananthpuram"]
#df.drop(["deviceid","quality","timestamp","EventProcessedUtcTime","PartitionId","EventEnqueuedUtcTime","IoTHub"],axis=1,inplace=True)
df["ISTtime"]=df["ISTtime"].astype("datetime64[ns]")
df["Date"]=pd.DatetimeIndex(df["ISTtime"]).date
df["Date"]=df["Date"].astype("datetime64[ns]")
df["Time"]=pd.DatetimeIndex(df["ISTtime"]).time
df["Hour"]=pd.DatetimeIndex(df["ISTtime"]).hour
df["Minute"]=pd.DatetimeIndex(df["ISTtime"]).minute
df["value"]=df["value"].astype("float64")
print(df.dtypes)
Radiation_Block1=df[(df['itemname']=="WMS_GII") & (df["Date"]==str(df["Date"].mode()[0]).split(" ")[0]) &  (df['value']>=2) & (df["Hour"]>=6) & (df["Hour"]<20)]
Radiation_Block1=Radiation_Block1[["ISTtime","itemname","value"]]
Radiation_Block1["ISTtime"]=Radiation_Block1["ISTtime"].astype("datetime64[ns]")
Radiation_Block1["Hour"]=pd.DatetimeIndex(Radiation_Block1["ISTtime"]).hour
Radiation_Block1["Minute"]=pd.DatetimeIndex(Radiation_Block1["ISTtime"]).minute
Data_To=str(Radiation_Block1["ISTtime"].head(1).iloc[0])
print(Data_To)
Data_From=str(Radiation_Block1["ISTtime"].tail(1).iloc[0])
print(Data_From)
df=df[(df["ISTtime"]>=Data_To) & (df["ISTtime"]<=Data_From)]
#df=df[ (df["Hour"]>=6) & (df["Hour"]<=19)]
#df=df[ (df["Hour"]>=Up_Hour) & (df["Hour"]<=Sleep_Hour) & (df["Minute"]>=Up_Minute) & (df["Minute"]<=Sleep_Minute)]
df=df.drop_duplicates()
df["value"]=df["value"].astype("float32")
Up_Hour=df[(df["itemname"]=="WMS_GII") & (df["value"]>=2) ].head(1)["Hour"].iloc[0]
Sleep_Hour=df[(df["itemname"]=="WMS_GII") & (df["value"]>=2) ].tail(1)["Hour"].iloc[0]
Up_Minute=df[(df["itemname"]=="WMS_GII") & (df["value"]>=2) ].head(1)["Minute"].iloc[0]
Sleep_Minute=df[(df["itemname"]=="WMS_GII") & (df["value"]>=2) ].tail(1)["Minute"].iloc[0]
#df.to_excel("abfss://devlop@ayanadatalake.dfs.core.windows.net/Phelan.xlsx")
current_time=datetime_Now.strftime('%Y-%m-%d %H:%M:%S.%f')
print(current_time)
before_time=before_time.strftime('%Y-%m-%d %H:%M:%S.%f')
before_time
five_min_before_time=five_min_before_time=(datetime.now(tz_india)-timedelta(minutes=8)).strftime('%Y-%m-%d %H:%M:%S.%f') # change time detla according to frequency of alert
#before_time.strftime('%Y-%m-%d %H:%M:%S.%f')

df=df[(df["ISTtime"]<=str(current_time)) & (df["ISTtime"]>=str(before_time))]
df

df_scb=df.copy()
df1=df.copy()
df2=df.copy()

Radiation_Block1

df["SPP"]="Phelan"
df=df[df['itemname'].str.startswith('ITC', na=True)]
df=df[df['itemname'].str.contains('TODAY_KWH', na=True)]
df = df[~df["itemname"].str.contains('MFM_', na=True)]
df = df[~df["itemname"].str.contains('MOD', na=True)]
inv_current_list=list(df["itemname"].unique())
inv_current_list

Block1_list=['ITC1_INV1_TODAY_KWH',
 'ITC3_INV1_TODAY_KWH',
 'ITC4_INV1_TODAY_KWH',
 'ITC2_INV1_TODAY_KWH',
 'ITC1_INV2_TODAY_KWH',
 'ITC3_INV2_TODAY_KWH',
 'ITC4_INV2_TODAY_KWH',
 'ITC2_INV2_TODAY_KWH',
 'ITC3_INV3_TODAY_KWH',
 'ITC1_INV3_TODAY_KWH',
 'ITC4_INV3_TODAY_KWH',
 'ITC2_INV3_TODAY_KWH',
 'ITC3_INV4_TODAY_KWH',
 'ITC1_INV4_TODAY_KWH',
 'ITC2_INV4_TODAY_KWH',
 'ITC4_INV4_TODAY_KWH']

list_of_down_inv=[ i for i in Block1_list if i not in inv_current_list ]
list_of_down_inv

columns=["Tagname"]
df_inv=pd.DataFrame(list_of_down_inv,columns=columns)
df_inv.to_excel("C:\\Users\\Piyush\\OneDrive\\LocalD\\Bhdl\\BhdlInv_Alerts.xlsx",index=False)

if len(list_of_down_inv)>0:
    import pymsteams
    myTeamsMessage = pymsteams.connectorcard("https://ayanapower.webhook.office.com/webhookb2/17435f69-7867-4a41-b5f0-46ceb0fb534f@59b60474-e282-44b5-881c-bb9ce815690c/IncomingWebhook/ae70209452044d1bb0c5a07451e8c1d6/0a64ab19-2984-4746-9447-21882231bb32")
    myTeamsMessage.text(str(list_of_down_inv))  
    myTeamsMessage.send()