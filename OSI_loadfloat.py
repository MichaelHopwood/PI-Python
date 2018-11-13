# -*- coding: utf-8 -*-
"""
Created on Tue Sep 11 11:14:31 2018

@author: Michael Hopwood
"""

import sys
import clr

import pandas as pd
import numpy as np
import datetime
import mysql.connector
import time
#from matplotlib import pyplot
import matplotlib.pyplot as plt

sys.path.append('C:\\Program Files (x86)\\PIPC\\AF\\PublicAssemblies\\4.0')  
clr.AddReference('OSIsoft.AFSDK')

from OSIsoft.AF import *
from OSIsoft.AF.PI import *
from OSIsoft.AF.Asset import *
from OSIsoft.AF.Data import *
from OSIsoft.AF.Time import *
from OSIsoft.AF.UnitsOfMeasure import *

# PI Data Archive
piServers = PIServers()
piServer = piServers['net1552.net.ucf.edu']

def Summarize_PI_Data(pitag, start, end, freq, timestampcalc = AFTimestampCalculation.MostRecentTime, summarytype = AFSummaryTypes.Maximum):
    '''Creates dataframe of historical max hourly values for a single PI point'''
    piServers = PIServers()
    piServer = piServers['net1552.net.ucf.edu']
    pt = PIPoint.FindPIPoint(piServer, pitag)
    print(type(pt))
    timerange = AFTimeRange(start,end)
    span = AFTimeSpan.Parse(freq)
    summaries = pt.Summaries(timerange, span, summarytype, AFCalculationBasis.TimeWeighted, timestampcalc)
    print(summaries)
    #print("Values updated at: ", datetime.datetime.now())
    # Loop through and make list
    times = []
    vals = []
    for summary in summaries:
        for event in summary.Value:
            times.append(str(event.Timestamp.LocalTime))
            if type(event.Value) is PIException:
                vals.append(None)
            else:
                vals.append(event.Value)
    print(type(vals))
    # Create dataframe
    df = pd.DataFrame(data = {pitag: vals}, index=times)
    df.index = pd.to_datetime(df.index)


def Summarize_Multi_PIData(pitags, start, end, freq, timestampcalc = AFTimestampCalculation.MostRecentTime, complete_cases = False, summarytype = AFSummaryTypes.Maximum):
    #Creates a dataframe with historical data for multiple points
    mult_df = pd.DataFrame()
    
    for tag in pitags:    
        df = Summarize_PI_Data(tag, start, end, freq, timestampcalc=timestampcalc, summarytype=summarytype)
        mult_df = pd.concat([mult_df, df], axis=1, join = 'outer')
        mult_df.index = pd.to_datetime(mult_df.index)
      
    if complete_cases:
        mult_df = mult_df.dropna(axis=0, how='any')
    return mult_df

def get_tag_values(pitag,timestart,timeend):
    piServers = PIServers()
    piServer = piServers['net1552.net.ucf.edu']
    tag = PIPoint.FindPIPoint(piServer, pitag) 
    timeRange = AFTimeRange.Parse(timestart, timeend)
    boundary = AFBoundaryType.Inside
    data = tag.RecordedValues(timeRange,boundary,'',False,0)
    dataList = list(data)
    results = np.zeros((len(dataList), 2), dtype='object')
    for i, sample in enumerate(data):
        results[i, 0] = float(sample.Value)
        results[i, 1] = str(sample.Timestamp.ToString("MM/dd/yyyy HH:mm:ss.fff"))
    
    df = pd.DataFrame(data=results[0:,0], index = results[0:,1], columns = [pitag]) 
    return df

def get_mult_values(pitaglist, timestart, timeend):
    mult_df = pd.DataFrame()
    
    for pitag in pitaglist:
        df = get_tag_values(pitag,timestart,timeend)
        mult_df = pd.concat([mult_df, df], axis = 1, join = 'outer')

    return mult_df

def get_IV(IVlist, trace_id, timestart, timeend, skipNaN = True):
    timeList = []
    df_traceID = get_tag_values(trace_id, timestart, timeend)
    trace_list = list(df_traceID.index.values)

    for IVstart in trace_list:
        IVstart = pd.to_datetime(IVstart).strftime('%m-%d-%Y %H:%M:%S')
        IVend = (pd.to_datetime(IVstart) + pd.Timedelta(seconds=4)).strftime('%m-%d-%Y %H:%M:%S')
        timeList.append([IVstart, IVend])
        
    df_list = []
    for i in range(len(timeList)):
        df_i = get_tag_values(IVlist[0], timeList[i][0], timeList[i][1])
        df_v = get_tag_values(IVlist[1], timeList[i][0], timeList[i][1])
        df = pd.concat([df_i, df_v], axis = 1, join = 'outer', sort = True)
        df_list.append(df)

    badIndexes = []
    for k in range(len(df_list)):
        if df_list[k].isnull().values.any() == True:
            print("There are NaN values in this dataframe. Beware!\nInvestigate to see where the NaN values are to resolve the issue.")
            if skipNaN == True:
                badIndexes.append(k)

    for x in range(len(df_list)):
        for y in range(len(badIndexes)):
            if badIndexes[y] == x:
                del df_list[x]
                print("removing element: ", x)

    return df_list

def calculate_Rsh(IVcurve):
    I = '8157_UCF.UCF_Inverter_1.CB_1.S_1.IV_I'
    V = '8157_UCF.UCF_Inverter_1.CB_1.S_1.IV_V'
    
    slope1 = (IVcurve[I][100] - IVcurve[I][0]) / (IVcurve[V][100] - IVcurve[V][0])
    Rsh1 =  -1 / slope1
    print("1:", Rsh1)
    
    slope2 = (IVcurve[I][75] - IVcurve[I][0]) / (IVcurve[V][75] - IVcurve[V][0])
    Rsh2 =  -1 / slope2
    print("2:", Rsh2)
    
    slope3 = (IVcurve[I][50] - IVcurve[I][0]) / (IVcurve[V][50] - IVcurve[V][0])
    Rsh3 =  -1 / slope3
    print("3:", Rsh3)
    
    return
        
def rename__cols(df):
    new_colnames = []
    for col in df.columns:
        new_colnames.append((col.split(".")[0]))
    df.columns = new_colnames
    return df

def Store_Vals(df, valuecol, pointname):
    '''
    Store values into the PI System
    df: dataframe that will be stored
    valuecol: column name that is to be stored for the point
    pointname: name of the point where this data will be published
    '''
    #Function for storing values from a dataframe back into PI. Index of the dataframe needs to be in 
    #datetime format
    df.rename(columns = {valuecol:'vals'}, inplace = True)
    piServer = piServers.DefaultPIServer
    writept = PIPoint.FindPIPoint(piServer,pointname)
    writeptname = writept.Name.lower()
    
    for row in df.itertuples():
        time.sleep(0.0005)
        val = AFValue()
        val.Value = float(row.vals)
        timed = AFTime(str(row.Index))
        val.Timestamp = timed  
        writept.UpdateValue(val, AFUpdateOption.Replace, AFBufferOption.BufferIfPossible)
        
def run_mysql(query, host, port, database, username, password):
    '''Connect to the MySQL server with specified information'''
    
    mydb = mysql.connector.connect(
        host = host,
        port = port,
        user = username,
        passwd = password,
        database = database
        )
    
    mycursor = mydb.cursor()
    mycursor.execute(query)
    
    data = mycursor.fetchall()
    mydb.close()
    return data        

def get_mysql_data(trace_id, string):

    # query the trace_id in mySQL
    trace_id_query = "SELECT CAST(datetime AS datetime), CAST(groupchannel_name as BINARY), trace_id FROM iv.trace WHERE groupchannel_name = " +f'"{string}"'  + "ORDER BY datetime DESC;"
    traceid_list = run_mysql(trace_id_query, host, port, database, username, password)
    
    # Create dataframe of datetime, groupchannel_name, and trace_id
    df_trace = pd.DataFrame(traceid_list, columns=['datetime', 'group', 'trace_id'])
    return df_trace

    
if __name__ == "__main__":
    pitaglist = ['8157_UCF.UCF_Inverter_1.CB_1.S_1.IV_I', '8157_UCF.UCF_Inverter_1.CB_1.S_1.IV_V']
    IV_trace = '8157_UCF.UCF_Inverter_1.CB_1.S_1.Isc'
    timestart = '11/07/2018 1:00:00 PM'
    timeend = '11/07/2018 4:30:00 PM'

    results = get_IV(pitaglist, IV_trace, timestart, timeend, skipNaN = True)

    
        
    