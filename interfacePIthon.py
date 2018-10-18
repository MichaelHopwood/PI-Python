# -*- coding: utf-8 -*-
"""
Created on Tue Oct 16 13:39:52 2018

@author: Michael Hopwood
"""
import sys
import clr # Connecting with .NET (PI Database)

import pandas as pd
import numpy as np
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

def Pull_PI_Data(pitag, start, end, freq, timestampcalc = AFTimestampCalculation.MostRecentTime, summarytype = AFSummaryTypes.Maximum):
    '''Creates dataframe of historical max hourly values for a single PI point'''
    piServers = PIServers()
    piServer = piServers['net1552.net.ucf.edu']
    pt = PIPoint.FindPIPoint(piServer, pitag)
    timerange = AFTimeRange(start,end)
    span = AFTimeSpan.Parse(freq)
    summaries = pt.Summaries(timerange, span, summarytype, AFCalculationBasis.TimeWeighted, timestampcalc)
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
    # Create dataframe
    df = pd.DataFrame(data = {pitag: vals}, index=times)
    df.index = pd.to_datetime(df.index)
    
def Store_Vals(df, valuecol, pointname):
    #Function for storing values from a dataframe back into PI. Index of the dataframe needs to be in 
    #datetime format
    df.rename(columns = {valuecol:'vals'}, inplace = True)
    df.head()
    piServer = piServers.DefaultPIServer
    writept = PIPoint.FindPIPoint(piServer,pointname)
    writeptname = writept.Name.lower()
    
    for row in df.itertuples():
        val = AFValue()
        val.Value = float(row.vals)
        time = AFTime(str(row.Index))
        val.Timestamp = time  
        writept.UpdateValue(val, AFUpdateOption.Replace, AFBufferOption.BufferIfPossible)
        
def run_mysql(query, host, port, database, username, password):
    "description description description" 
    
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
        
if __name__ == "__main__":
    
    # Set the variables needed for run_mysql function
    host = '74.235.221.139'
    port = '7213'
    username = 'pordis'
    password = 'H)k6S!s5@2'
    database = 'iv'
    
    # Query the trace_id in MySQL
    trace_id_query_S1 = "SELECT CAST(datetime AS datetime), CAST(groupchannel_name as BINARY), trace_id FROM iv.trace ORDER BY datetime DESC;"
    start1_time =time.time()
    S1_traceid_list = run_mysql(trace_id_query_S1, host, port, database, username, password)
    print("Query trace ID time\n--- %s seconds ---" % (time.time() - start1_time))

    # Query the current trace_id in the PI System
    S1_PI_traceid_df = Pull_PI_Data('8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_1.trace_id', '*-1m', '*', '1m')
 
    # If no value present in PI System, set trace_id locally to 1
    if S1_PI_traceid_df is None:
        S1_PI_traceid = 1
    else:
        S1_PI_traceid = S1_PI_traceid_df[S1_PI_traceid_df.columns[0]][0]

    # Create dataframe of datetime, groupchannel_name, and trace_id
    df_trace = pd.DataFrame(S1_traceid_list, columns=['datetime', 'group', 'trace_id'])

    # Get index of sql table where the last pi value is located  
    index_PiVal = df_trace[df_trace['trace_id'] == S1_PI_traceid].index.tolist()
    index_query = str(index_PiVal[0])

    # concatenate SQL query to have the index_query length 
    S1query = "SELECT CAST(datetime AS datetime), CAST(groupchannel_name as BINARY), trace_id, exclude, current, voltage FROM iv.trace WHERE groupchannel_name = '8157_S1' ORDER BY datetime DESC LIMIT 0, " + index_query + ";"
    
    # Run this query
    start_time = time.time()
    S1_temp_list = run_mysql(S1query, host, port, database, username, password)
    print("Query render time\n--- %s seconds ---\n" % (time.time() - start_time))

    # Create dataframe of datetime, groupchannel_name, trace_id, exclude, current, voltage
    # Current = IV_I in the PI System
    # Voltage = IV_V in the PI System
    df = pd.DataFrame(S1_temp_list, columns=['datetime', 'group', 'trace_id', 'exclude', 'current', 'voltage'])
    
    # drop rows where exclude = 1
    df["exclude"] = df["exclude"].apply(lambda x: np.NaN if x == 1 else x)
    df.dropna(0, how='any', inplace=True)
    
    # Ensure that dataframe is in order
    df.sort_values(['trace_id'], inplace=True)

    # Create dataframe and parse out the current values with correct datetime
    # Datetime will be an incrementing millisecond for each value in list 
    # at a certain timestamp
    
    # S1 Current dataframe --- Currently in Beta
    current_df = pd.DataFrame(columns=['datetime', 'current'])  
    start2_time =time.time()    
    i = 0
    previous_datetime = 0
    for _, row in df.iterrows():
        for value in row['current'].split(','):
            if previous_datetime != pd.to_datetime(row['datetime']):
                i = 0
                previous_datetime = pd.to_datetime(row['datetime'])
            else:
                i += 1
            current_df = current_df.append({'datetime': pd.to_datetime(row['datetime']) + pd.to_timedelta(f"{i}ms"), 'current': value}, ignore_index=True)
    print("Creating dataframe for current time\n--- %s seconds ---\n" % (time.time() - start2_time))
 
    # S1 Voltage dataframe --- NOT COMPLETED
    current_df = pd.DataFrame(columns=['datetime', 'current'])  
    start2_time =time.time()    
    i = 0
    previous_datetime = 0
    for _, row in df.iterrows():
        for value in row['current'].split(','):
            if previous_datetime != pd.to_datetime(row['datetime']):
                i = 0
                previous_datetime = pd.to_datetime(row['datetime'])
            else:
                i += 1
            current_df = current_df.append({'datetime': pd.to_datetime(row['datetime']) + pd.to_timedelta(f"{i}ms"), 'current': value}, ignore_index=True)
    print("Creating dataframe for current time\n--- %s seconds ---\n" % (time.time() - start2_time))
    
    # S2 Current dataframe ----NOT COMPLETED
    current_df = pd.DataFrame(columns=['datetime', 'current'])  
    start2_time =time.time()    
    i = 0
    previous_datetime = 0
    for _, row in df.iterrows():
        for value in row['current'].split(','):
            if previous_datetime != pd.to_datetime(row['datetime']):
                i = 0
                previous_datetime = pd.to_datetime(row['datetime'])
            else:
                i += 1
            current_df = current_df.append({'datetime': pd.to_datetime(row['datetime']) + pd.to_timedelta(f"{i}ms"), 'current': value}, ignore_index=True)
    print("Creating dataframe for current time\n--- %s seconds ---\n" % (time.time() - start2_time))
    
    # S2 Voltage dataframe ----NOT COMPLETED
    current_df = pd.DataFrame(columns=['datetime', 'current'])  
    start2_time =time.time()    
    i = 0
    previous_datetime = 0
    for _, row in df.iterrows():
        for value in row['current'].split(','):
            if previous_datetime != pd.to_datetime(row['datetime']):
                i = 0
                previous_datetime = pd.to_datetime(row['datetime'])
            else:
                i += 1
            current_df = current_df.append({'datetime': pd.to_datetime(row['datetime']) + pd.to_timedelta(f"{i}ms"), 'current': value}, ignore_index=True)
    print("Creating dataframe for current time\n--- %s seconds ---\n" % (time.time() - start2_time))
    
    
    # eliminate Nan values that were created by extra comma at end of each list
    current_df.dropna(0, how='any', inplace=True)
    
    # publish current, voltage, trace_id for S1 and S2
    Store_Vals(current_df, 'current', '8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_1.IV_I')
    Store_Vals(current_df, 'voltage', '8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_1.IV_I')
    Store_Vals(current_df, 'current', '8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_2.IV_I')
    Store_Vals(current_df, 'voltage', '8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_2.IV_I')