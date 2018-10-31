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
    
def get_tag_snapshot(tagname):  
    piServers = PIServers()
    piServer = piServers['net1552.net.ucf.edu']
    tag = PIPoint.FindPIPoint(piServer, tagname)  
    lastData = tag.Snapshot()

    if str(type(lastData.Value)) == "<class 'OSIsoft.AF.Asset.AFEnumerationValue'>":
        value = 'no val'
    else:
        value = lastData.Value
    return value

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

def get_mysql_data(trace_id, parameter, string):
    '''
    Parameters:
    trace_id: specify the trace_id that is used    
    parameter: str, 'current' or 'voltage'
    string: str, '8157_S1' or '8157_S2'
    '''

    # query the trace_id in mySQL
    trace_id_query = "SELECT CAST(datetime AS datetime), CAST(groupchannel_name as BINARY), trace_id FROM iv.trace WHERE groupchannel_name = " +f'"{string}"'  + "ORDER BY datetime DESC;"
    start1_time =time.time()
    traceid_list = run_mysql(trace_id_query, host, port, database, username, password)
    f.write("Query trace ID time\n--- %s seconds ---\n" % (time.time() - start1_time))
    f.flush()
    
    # Create dataframe of datetime, groupchannel_name, and trace_id
    df_trace = pd.DataFrame(traceid_list, columns=['datetime', 'group', 'trace_id']) 
    
    # If no value present in PI System, set trace_id locally to 1240
    # else, use the last value in PI System
    if get_tag_snapshot(trace_id) == 'no val':
        if string == '8157_S1':
            cur_traceid = 1241
        elif string == '8157_S2':
            cur_traceid = 1240
    else:
        cur_traceid = int(get_tag_snapshot(trace_id))

    f.write("Using trace_id: {0}\n".format(cur_traceid))
    f.flush()

    # Get index of mysql table where the last pi value is located  
    index_PiVal = df_trace[df_trace['trace_id'] == cur_traceid].index.tolist()
    
    # String value of latest index in PI System
    index_query = str(index_PiVal[0])

    # If no update from table, stop everything
    if index_query == '0':
        f.write("No updated values.\nSleeping until next prompt...\n")
        f.flush()
        sys.exit(1)
    
    # concatenate SQL query to have the index_query length 
    query = "SELECT CAST(datetime AS datetime), CAST(groupchannel_name as BINARY), trace_id, exclude, " + parameter + " FROM iv.trace WHERE groupchannel_name = " +f'"{string}"'  + " ORDER BY datetime DESC LIMIT 0, " + index_query + ";"

    # Run this query
    start_time = time.time()
    temp_list = run_mysql(query, host, port, database, username, password)
    f.write("Query render time\n--- %s seconds ---\n" % (time.time() - start_time))
    f.flush()
    #Create dataframe of parameters in query
    df = pd.DataFrame(temp_list, columns=['datetime', 'group', 'trace_id', 'exclude', parameter])

    # drop rows where exclude = 1
    df["exclude"] = df["exclude"].apply(lambda x: np.NaN if x == 1 else x)
    df.dropna(0, how='any', inplace=True)
    
    # Ensure that dataframe is in order
    df.sort_values(['trace_id'], inplace=True)
    df.set_index(df['datetime'], inplace=True)

    return df
    
    
def reformat_IV(df):
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
    f.write("Creating dataframe for current time\n--- %s seconds ---\n\n" % (time.time() - start2_time))
    f.flush()
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
    now = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    f.write('\n=========================================\n')
    f.write('Start of iteration:\n{0}\n'.format(now))
    f.write('=========================================\n')
    f.flush()
    # string 1, current
    trace_id = '8157_UCF.UCF_Inverter_1.CB_1.S_1.trace_id'
    string = '8157_S1'
    parameter = 'current'
    df = get_mysql_data(trace_id, parameter, string)
    output_df= reformat_IV(df)
    Store_Vals(output_df, parameter, '8157_UCF.UCF_Inverter_1.CB_1.S_1.IV_I')
     
    # string 1, voltage
    string = '8157_S1'
    parameter = 'voltage'
    df = get_mysql_data(trace_id, parameter, string)
    output_df= reformat_IV(df)
    Store_Vals(output_df, parameter, '8157_UCF.UCF_Inverter_1.CB_1.S_1.IV_V')   
    
    # string 1, trace_id
    Store_Vals(df, 'trace_id', trace_id)
    
    # string 2, current
    trace_id = '8157_UCF.UCF_Inverter_1.CB_1.S_2.trace_id'
    string = '8157_S2'
    parameter = 'current'
    df = get_mysql_data(trace_id, parameter, string)
    output_df= reformat_IV(df)
    Store_Vals(output_df, parameter, '8157_UCF.UCF_Inverter_1.CB_1.S_2.IV_I')  
    
    # string 2, voltage
    string = '8157_S2'
    parameter = 'voltage'
    df = get_mysql_data(trace_id, parameter, string)
    output_df= reformat_IV(df)
    Store_Vals(output_df, parameter, '8157_UCF.UCF_Inverter_1.CB_1.S_2.IV_V')
    
    # string 2, trace_id
    Store_Vals(df, 'trace_id', trace_id)
    
    # Calculate number of trace_ids
    num_trace = df['vals'][-1] - df['vals'][0]
    now_1 = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    
    # Calculate total script duration in seconds
    delta = (pd.to_datetime(now_1) - pd.to_datetime(now)).total_seconds()
    
    f.write('Average rate per trace_id: {0:.2f}\n'.format((delta / num_trace) / 60))
    f.write('\nEnd of iteration:\n{0}\n'.format(now_1))
    f.close()
    
