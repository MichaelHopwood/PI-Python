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
import datetime

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
    '''
    Store values into the PI System
    df: dataframe that will be stored
    valuecol: column name that is to be stored for the point
    pointname: name of the point where this data will be published
    '''
    #Function for storing values from a dataframe back into PI. Index of the dataframe needs to be in 
    #datetime format
    df.rename(columns = {valuecol:'vals'}, inplace = True)
    df.head()
    piServer = piServers.DefaultPIServer
    writept = PIPoint.FindPIPoint(piServer,pointname)
    writeptname = writept.Name.lower()
    
    for row in df.itertuples():
        time.sleep(0.010)
        val = AFValue()
        val.Value = float(row.vals)
        timed = AFTime(str(row.Index))
        val.Timestamp = timed  
        writept.UpdateValue(val, AFUpdateOption.Replace, AFBufferOption.Buffer)
        
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

def reformat_IV(trace_id, parameter, string):
    '''
    Parameters:
    trace_id: specify the trace_id that is used    
    parameter: str, 'current' or 'voltage'
    string: str, '8157_S1' or '8157_S2'
    '''

    # query the trace_id in mySQL
    trace_id_query = "SELECT CAST(datetime AS datetime), CAST(groupchannel_name as BINARY), trace_id FROM iv.trace ORDER BY datetime DESC;"
    start1_time =time.time()
    traceid_list = run_mysql(trace_id_query, host, port, database, username, password)
    f.write("Query trace ID time\n--- %s seconds ---\n" % (time.time() - start1_time))
    
    
    # Query the current trace_id in the PI System
    S1_PI_traceid_df = Pull_PI_Data(trace_id, '*-1m', '*', '1m')

    # If no value present in PI System, set trace_id locally to 1
    # else, use the last value in PI System
    if S1_PI_traceid_df is None:
        S1_PI_traceid = 1
    else:
        S1_PI_traceid = S1_PI_traceid_df[S1_PI_traceid_df.columns[0]][0]
        
    # Create dataframe of datetime, groupchannel_name, and trace_id
    df_trace_S1 = pd.DataFrame(traceid_list, columns=['datetime', 'group', 'trace_id'])


    # Get index of sql table where the last pi value is located  
    index_PiVal_S1 = df_trace_S1[df_trace_S1['trace_id'] == S1_PI_traceid].index.tolist()
    
    # String value of latest index in PI System
    index_query_S1 = str(index_PiVal_S1[0])
    
    # concatenate SQL query to have the index_query length 
    S1query = "SELECT CAST(datetime AS datetime), CAST(groupchannel_name as BINARY), trace_id, exclude, " + parameter + " FROM iv.trace WHERE groupchannel_name = " +f'"{string}"'  + " ORDER BY datetime DESC LIMIT 0, " + index_query_S1 + ";"
    print("here")
    # Run this query
    start_time = time.time()
    S1_temp_list = run_mysql(S1query, host, port, database, username, password)
    f.write("Query render time\n--- %s seconds ---\n" % (time.time() - start_time))

    #Create dataframe of parameters in query
    df = pd.DataFrame(S1_temp_list, columns=['datetime', 'group', 'trace_id', 'exclude', parameter])

    # drop rows where exclude = 1
    df["exclude"] = df["exclude"].apply(lambda x: np.NaN if x == 1 else x)
    df.dropna(0, how='any', inplace=True)
    
    # Ensure that dataframe is in order
    df.sort_values(['trace_id'], inplace=True)
    
    '''Create dataframe and parse out the current values with correct datetime
     Datetime will be an incrementing millisecond for each value in list 
     at a certain timestamp
    '''
    output_df = pd.DataFrame(columns=['datetime', parameter])
    start2_time =time.time()    
    i = 0
    previous_datetime = 0
    for _, row in df.iterrows():
        for value in row[parameter].split(','):
            if previous_datetime != pd.to_datetime(row['datetime']):
                i = 0
                previous_datetime = pd.to_datetime(row['datetime'])
            else:
                i += 1
            output_df = output_df.append({'datetime': pd.to_datetime(row['datetime']) + pd.to_timedelta(f"{i}ms"), parameter: value}, ignore_index=True)
    f.write("Creating dataframe for current time\n--- %s seconds ---\n" % (time.time() - start2_time))

    # eliminate Nan values that were created by extra comma at end of each list
    output_df.dropna(0, how='any', inplace=True)
    
    # set index equal to datetime column
    output_df.set_index(output_df['datetime'], inplace=True)

    return output_df


if __name__ == "__main__":
    
    # Set the variables needed for run_mysql function
    host = ''
    port = ''
    username = ''
    password = ''
    database = ''
    
    # open logging file
    f = open('interface_logs.txt', 'a+')
    f.write('\nStart of iteration:\n{0}\n'.format(datetime.datetime.now().time()))

    # string 1, current
    trace_id = '8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_1.trace_id'
    string = '8157_S1'
    parameter = 'current'
    output_df = reformat_IV(trace_id, parameter, string)
    Store_Vals(output_df, parameter, '8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_1.IV_I')

    # string 1, voltage
    string = '8157_S1'
    parameter = 'voltage'
    output_df = reformat_IV(trace_id, parameter, string)
    Store_Vals(output_df, parameter, '8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_1.IV_V')   
    
    # string 2, current
    trace_id = '8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_2.trace_id'
    string = '8157_S2'
    parameter = 'current'
    output_df = reformat_IV(trace_id, parameter, string)
    Store_Vals(output_df, parameter, '8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_2.IV_I')  
    
    # string 2, voltage
    string = '8157_S2'
    parameter = 'voltage'
    output_df = reformat_IV(trace_id, parameter, string)
    Store_Vals(output_df, parameter, '8157_UCF_FSEC.UCF_Inverter_1.CB_1.S_2.IV_V')


    f.write('\nEnd of iteration:\n{0}\n'.format(datetime.datetime.now().time()))
    f.write('sleeping until next prompt...')
    
    f.close()
    
