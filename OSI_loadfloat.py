# -*- coding: utf-8 -*-
"""
Created on Tue Sep 11 11:14:31 2018

@author: Michael Hopwood
"""

import sys
import clr # Connecting with .NET (PI Database)

import pandas as pd
import numpy as np
import datetime
import mysql.connector
import time


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


# Still IN BETA
def get_timestamps(pitag, start, end, freq, timestampcalc = AFTimestampCalculation.MostRecentTime, summarytype = AFSummaryTypes.Maximum):
    '''Creates dataframe of historical max hourly values for a single PI point'''
    piServers = PIServers()
    piServer = piServers['net1552.net.ucf.edu']
    pt = PIPoint.FindPIPoint(piServer, pitag)
    timerange = AFTimeRange(start,end)
    span = AFTimeSpan.Parse(freq)
    summaries = pt.Summaries(timerange, span, summarytype, AFCalculationBasis.TimeWeighted, timestampcalc)
    #print("Values updated at: ", datetime.datetime.now())
    # Loop through and make list
    times = []
    vals = []     
    lastValue = 0
    for summary in summaries:
        for event in summary.Value:
            times.append(str(event.Timestamp.LocalTime))
            if int(event.Value) > int(lastValue):
                if get_tag_values(pitag, (pd.to_datetime(str(event.Timestamp.LocalTime)) - pd.Timedelta('+1s')).strftime('%m/%d/%Y %H:%M:%S'), (pd.to_datetime(str(event.Timestamp.LocalTime)) + pd.Timedelta('+1s')).strftime('%m/%d/%Y %H:%M:%S')) is not None:
                    vals.append(event.Value)
            event.Value = lastValue

    # Create dataframe
    df = pd.DataFrame(data = {pitag: vals}, index=times)
    df.index = pd.to_datetime(df.index)

    return df


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
    tag = PIPoint.FindPIPoint(piServer, pitag) 
    timeRange = AFTimeRange(timestart,timeend)
    boundary = AFBoundaryType.Inside
    data = tag.RecordedValues(timeRange,boundary,'',False,0)
    dataList = list(data)
    #print len(dataList)
    results = np.zeros((len(dataList), 1), dtype='object') #numpy array
    for i, sample in enumerate(data):
        results[i, :] = float(sample.Value)
    
    df = pd.DataFrame(data=results[0:,0:], columns = [pitag]) 
    return df

# Currently in BETA
def Pull_IV(IVtags, IVtrace, string, start, end):
    #Creates a dataframe with historical data for multiple points
    mult_df = pd.DataFrame()
    df_trace = get_mysql_data(IVtrace, string)
    df_trace.index = df_trace['datetime']
    # convert to unix
#    df_trace['datetime'].astype(np.int64) // 10**9
    trace_series = pd.to_datetime(df_trace['datetime']).astype(np.int64) // 10**6
    df_trace_unix = trace_series.to_frame()
#    start = int(time.mktime(pd.to_datetime(start).timetuple()))
#    end = int(time.mktime(pd.to_datetime(end).timetuple()))
    
    print(df_trace['datetime'])
    print(start)
    print(end)
    
    df_query = df_trace_unix[(df_trace_unix['datetime'] > start) | (df_trace_unix['datetime'] < end)]
    for IVstart in df_query['datetime']:
        IVstart = str(IVstart)
        IVend = (pd.to_datetime(IVstart) + pd.Timedelta(seconds=3)).strftime('%m/%d/%Y %H:%M:%S')
        for pitag in IVtags:    
            df = get_tag_values(pitag, IVstart, IVend)
            mult_df = pd.concat([mult_df, df], axis=1, join = 'outer')

    return mult_df 

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
    host = ''
    port = ''
    username = ''
    password = ''
    database = ''

    
    start = '10/27/2018 12:00:00'
    end = '10/27/2018 1:00:00'
    IVtags = ['8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_1.IV_I', '8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_1.IV_V']
    IVtrace = '8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_2.trace_id'
    string = '8157_S1'
    Pull_IV(IVtags, IVtrace, string, start, end)

    
   