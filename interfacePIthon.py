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

def get_tag_values(pitag,timestart,timeend):
    tag = PIPoint.FindPIPoint(piServer, pitag) 
    timeRange = AFTimeRange(timestart,timeend)
    boundary = AFBoundaryType.Inside
    data = tag.RecordedValues(timeRange,boundary,'',False,0)
    dataList = list(data)
    results = np.zeros((len(dataList), 1), dtype='object')
    for i, sample in enumerate(data):
        results[i, :] = float(sample.Value)
    
    df = pd.DataFrame(data=results[0:,0:], columns = [pitag]) 
    return df

def Store_Vals(df, valuecol, pitag):
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
    writept = PIPoint.FindPIPoint(piServer,pitag)
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
    traceid_list = run_mysql(trace_id_query, host, port, database, username, password)
    
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
    query = "SELECT CAST(datetime AS datetime), CAST(groupchannel_name as BINARY), trace_id, num_points, exclude, " + parameter + " FROM iv.trace WHERE groupchannel_name = " +f'"{string}"'  + " ORDER BY datetime DESC LIMIT 0, " + index_query + ";"

    # Run this query
    temp_list = run_mysql(query, host, port, database, username, password)
    #Create dataframe of parameters in query
    df = pd.DataFrame(temp_list, columns=['datetime', 'group', 'trace_id', 'num_points', 'exclude', parameter])

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

    # eliminate Nan values that were created by extra comma at end of each list
    output_df.dropna(0, how='any', inplace=True)
    
    # set index equal to datetime column
    output_df.set_index(output_df['datetime'], inplace=True)
    
    return output_df

def dataQuality(pitag, trace_id, parameter, string):
    ''' A check of the data quality for mySQL to PI for the Trace data
    Quality is ensured by comparing the length of the list with the amount of
    data in each trace saved in the temporary memory AND the amount of data in
    each trace saved in the PI System
    '''
    pseudoname = pitag[29:]
    df = get_mysql_data(trace_id, parameter, string)
    dataList = df['num_points'].sum()
    output_df = reformat_IV(df)
    PI_df = get_tag_values(pitag, '*-3s', '*')
    
    status1 = 0
    status2 = 0
    
    if(dataList != len(output_df.index)):
        #mysql data length = reformatted data length
        status1 = 1

    if(dataList != len(PI_df.index)):
        #mysql data length = length of last trace data in PI
        status2 = 1
        
    if(status1 == 0 & status2 == 0):
        status = "\n", pseudoname, ": Data quality ensured."
        
    elif(status1 == 1 & status2 == 0):
        status = "\n", pseudoname, ": The reformatted data length does not equal the length of the extracted mySQL data. Error code: 1\n"
        
    elif(status1 == 0 & status2 == 1):
        status = "\n", pseudoname, ": The length of the data in the PI System for the last trace does not equal the length of the extracted mySQL data. Error code: 2\n"
    
    elif(status1 == 1 & status2 == 1):
        status = "\n", pseudoname, ": The reformatted data length AND the length of the data in the PI System for the last trace does not equal the length of the extracted mySQL data. Error code: 3\n"
        
    return status

def statusPrint(statusList, outputList):
    '''Print data quality status'''
    flag = 0
    flag1 = 0
    
    cur_time = datetime.datetime.now()
    
    #Check statuses created in dataQuality and print if error is shown
    for status in statusList:
        if(status[2][-2] == '1' or status[2][-2] == '2' or status[2][-2] == '3'):
            f.write(status)
            f.flush()
            flag = 1
            
    #Check if num values in current = num values in voltage
    for output in outputList:
        if(len(output[0]['vals']) != len(output[1]['vals'])):
            f.write('FAILURE: The number of values for Current does not equal the number of values for Voltage.\n')    
            f.flush()
            flag1 = 1
    
    #If no error, print out message and set flag to 0
    if ((all(status[2][-1] == '.' for status in statusList) == True) and flag == 0 and flag1 == 0):
        f.write('\nData quality ensured.\n')
        f.flush()
        goodDF = pd.DataFrame({'date': [cur_time], 'zeros': [0]})
        goodDF = goodDF.set_index('date')
        Store_Vals(goodDF, 'zeros', '8157_UCF.UCF_Inverter_1.CB_1.S_1.Python_IV_Flag')
        
    #If error was present, set flag to 1
    else:
        badDF = pd.DataFrame({'date': [cur_time], 'ones': [1]})
        badDF = goodDF.set_index('date')
        Store_Vals(badDF, 'ones', '8157_UCF.UCF_Inverter_1.CB_1.S_1.Python_IV_Flag')
        
    return
    
def extractTransformLoad(pitag, trace_id, parameter, string):
    ''' A function that modularizes the process of grabbing from mySQL, 
        reformatting with appropriate millisecond datetimes, and storing
        into the PI System
    '''
    df = get_mysql_data(trace_id, parameter, string)
    output_df= reformat_IV(df)
    Store_Vals(output_df, parameter, pitag)
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
    pitag = '8157_UCF.UCF_Inverter_1.CB_1.S_1.IV_I'
    outputC1 = extractTransformLoad(pitag, trace_id, parameter, string)
    statusC1 = dataQuality(pitag, trace_id, parameter, string)

    
    # string 1, voltage
    string = '8157_S1'
    parameter = 'voltage'
    pitag = '8157_UCF.UCF_Inverter_1.CB_1.S_1.IV_V'
    outputV1 = extractTransformLoad(pitag, trace_id, parameter, string)  
    statusV1 = dataQuality(pitag, trace_id, parameter, string)

    
    # string 1, trace_id
    df = get_mysql_data(trace_id, parameter, string)
    temp_list = df['trace_id'].tolist()
    traceIDs = ','.join(str(e) for e in temp_list)
    f.write("Using trace_id: {0}\n".format(traceIDs))
    f.flush()
    Store_Vals(df, 'trace_id', trace_id)

    
    # string 2, current
    trace_id = '8157_UCF.UCF_Inverter_1.CB_1.S_2.trace_id'
    string = '8157_S2'
    parameter = 'current'
    pitag = '8157_UCF.UCF_Inverter_1.CB_1.S_2.IV_I'
    outputC2 = extractTransformLoad(pitag, trace_id, parameter, string) 
    statusC2 = dataQuality(pitag, trace_id, parameter, string)

    
    # string 2, voltage
    string = '8157_S2'
    parameter = 'voltage'
    pitag = '8157_UCF.UCF_Inverter_1.CB_1.S_2.IV_V'
    outputV2 = extractTransformLoad(pitag, trace_id, parameter, string)
    statusV2 = dataQuality(pitag, trace_id, parameter, string)

    
    # string 2, trace_id
    df = get_mysql_data(trace_id, parameter, string)
    temp_list = df['trace_id'].tolist()
    traceIDs = ','.join(str(e) for e in temp_list)
    f.write("Using trace_id: {0}\n".format(traceIDs))
    f.flush()
    Store_Vals(df, 'trace_id', trace_id)
    

    # Calculate number of trace_ids
    num_trace = (df['vals'][-1] - df['vals'][0]) + 1
    now_1 = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    # Data quality status
    statusList = [statusC1, statusV1, statusC2, statusV2]
    outputList = [[outputC1, outputV1], [outputC2, outputV2]]
    statusPrint(statusList, outputList)

    # Calculate total script duration in seconds
    delta = (pd.to_datetime(now_1) - pd.to_datetime(now)).total_seconds()
    
    f.write('Average rate per trace_id: {0:.2f}\n'.format((delta / num_trace) / 60))
    f.write('\nEnd of iteration:\n{0}\n'.format(now_1))
    f.close()
    
