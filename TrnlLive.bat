@echo off
C:
CD C:\Users\Piyush\LocalDTrnl
START Python "C:\Users\Piyush\LocalDTrnl\Trnl_LocalDRealtimeDaily_Downstream.py"

CD ../
CD C:\Users\Piyush\LocalDTrnl
START Python "C:\Users\Piyush\LocalDTrnl\Trnl_LocalDRealtimeDaily_SCBAlerts.py"
