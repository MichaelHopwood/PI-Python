# -*- coding: utf-8 -*-
"""
Created on Tue Nov 20 16:39:58 2018

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

def get_any_tag_values(parameter, timestart = None, timeend = None, element = '8157_S1', save_csv = False, saveName = 'defaultName'):
    '''
    Get values for all tags from any source. 
    
    PARAMETERS:
        parameter: str, data name
            PI System data: tag data archive name
            mySQL data: IV trace data - input 'current' or 'voltage' for IV trace data
        timestart: str, datetime of intended start of query
            Used for PI System data query
        timeend: str, datetime of intended end of query
            Used for PI System data query
        element: str, specify which apparatus you want the data for
                 If you want data for everything, use 'any'
                 Current Possibilities: '8157_S1', '8157_S2', 'any'
                 
    EXAMPLES: 
    1. IF you want to query something including current or voltage from trace (IV_I, IV_V), the element parameter is required:
        
        get_any_tag_values(['current', 'voltage', '8157_UCF.UCF_Inverter_1.CB_1.S_1.Isc'], timestart = '10/05/2018 9:00:00 AM', timeend = '10/05/2018 4:00:00 PM', element = 'any')
            
    The list of strings are the column titles in the mySQL database
    
    2. IF you want to query something without including current or voltage from trace
        
        get_any_tag_values(['8157_UCF.UCF_Inverter_1.CB_1.S_1.Isc', '8157_UCF.UCF_Inverter_1.CB_1.S_1.Voc'], timestart = '10/05/2018 9:00:00 AM', timeend = '10/05/2018 4:00:00 PM')
    
    The list of strings are the tag names saved in the net1552.net.ucf.edu data archive
    
    ** IF CURRENT OR VOLTAGE FROM TRACE IS INCLUDED IN GROUP OF QUERY, MUST INCLUDE 'element' PARAMETER **
    '''
    
    groups = []
    results = []
    
    host = 'INPUT_IP_HERE'
    port = 'INPUT_PORT_HERE'
    username = 'INPUT_USERNAME_HERE'
    password = 'INPUT_PASSWORD_HERE'
    database = 'INPUT_DATABASE_HERE'
    
    trace_table_parameter_list = ['trace_id', 'datetime', 'groupchannel', 'groupchannel_name', 'trace_type', 'num_points', 'current', 'voltage', 'irradiance', 'aux', 'isc', 'voc', 'pmax', 'ipmax', 'vpmax', 'ff', 'avg_irradiance', 'avg_aux', 'Rch', 'Rsh', 'Rsh_r2', 'Rs', 'Rs_r2', 'additional_data', 'exclude', 'extract', 'analysis_datetime', 'analysis_code_version', 'analysis_classification', 'analysis_user_verification', 'analysis_i', 'analysis_v', 'analysis_isc', 'analysis_voc', 'analysis_pmax', 'analysis_ipmax', 'analysis_vpmax', 'analysis_ff']
    
    mysql_timestart = datetime.datetime.strptime(timestart, '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S')
    mysql_timeend = datetime.datetime.strptime(timeend, '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S')
    for i in range(len(parameter)):
        if(parameter[i] == 'current' or parameter[i] == 'voltage'):
            groups.append(1)
            if element == 'any':
                query = "SELECT datetime, groupchannel_name, " + parameter[i] + " FROM iv.trace WHERE (datetime BETWEEN " + f'"{mysql_timestart}"' + " AND " + f'"{mysql_timeend}"' + ");"
            else:
                query = "SELECT datetime, groupchannel_name, " + parameter[i] + " FROM iv.trace WHERE (datetime BETWEEN " + f'"{mysql_timestart}"' + " AND " + f'"{mysql_timeend}"' + ") AND (groupchannel_name = " + f'"{element}"' + ");"
            data = run_mysql(query, host, port, database, username, password)
            df = pd.DataFrame(data, columns=['datetime', 'group', parameter[i]])
        
        else:
            df = get_tag_values(parameter[i], timestart, timeend)
            groups.append(0)
        
        results.append(df)

    if save_csv is True:
        for i in range(len(results)):
            results[i].to_csv(saveName + '_' + parameter[i] + '_' + str(i) + '.csv')
    
    return results
    

def Summarize_PI_Data(pitag, start, end, freq, timestampcalc = AFTimestampCalculation.MostRecentTime, summarytype = AFSummaryTypes.Maximum):
    '''Creates dataframe of historical max hourly values for a single PI point'''
    
    piServers = PIServers()
    piServer = piServers['net1552.net.ucf.edu']
    pt = PIPoint.FindPIPoint(piServer, pitag)
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
    # Create dataframe
    df = pd.DataFrame(data = {pitag: vals}, index=times)
    df.index = pd.to_datetime(df.index)

def Summarize_Multi_PIData(pitags, start, end, freq, timestampcalc = AFTimestampCalculation.MostRecentTime, complete_cases = False, summarytype = AFSummaryTypes.Maximum):
    '''Creates a dataframe with historical data for multiple points'''
    mult_df = pd.DataFrame()
    
    for tag in pitags:    
        df = Summarize_PI_Data(tag, start, end, freq, timestampcalc=timestampcalc, summarytype=summarytype)
        mult_df = pd.concat([mult_df, df], axis=1, join = 'outer')
        mult_df.index = pd.to_datetime(mult_df.index)
    
    if complete_cases:
        mult_df = mult_df.dropna(axis=0, how='any')
    return mult_df

def get_tag_values(pitag,timestart,timeend):
    '''
    Get multiple tags from PI Point and return in a single dataframe
    
    Parameters:
        pitaglist: string of pi tag's data archive name
        timestart: str, datetime format of start time
        timeend: str, datetime format of end time
        
    Return:
        df: datetime index and column of queried tag
    '''
    piServers = PIServers()
    piServer = piServers['net1552.net.ucf.edu']
    tag = PIPoint.FindPIPoint(piServer, pitag)
    timeRange = AFTimeRange.Parse(timestart, timeend)
    boundary = AFBoundaryType.Inside
    data = tag.RecordedValues(timeRange,boundary,'',False,0)
    dataList = list(data)
    results = np.zeros((len(dataList), 2), dtype='object')
    for i, sample in enumerate(data):
#        print(sample)
        if str(sample.Value) != 'Calc Failed':
            results[i, 0] = str(sample.Value)
            results[i, 1] = str(sample.Timestamp.ToString("MM/dd/yyyy HH:mm:ss.fff"))
    

    df = pd.DataFrame(data=results[0:,0], index = results[0:,1], columns = [pitag]) 
    return df

def get_mult_values(pitaglist, timestart, timeend):
    '''
    Get multiple tags from PI Point and return in a single dataframe
    
    Parameters:
        pitaglist: list of str
        timestart: str, datetime format of start time
        timeend: str, datetime format of end time
        
    Return:
        mult_df: datetime index and columns of each queried tag
    '''
    mult_df = pd.DataFrame()
    
    for pitag in pitaglist:
        df = get_tag_values(pitag,timestart,timeend)
        mult_df = pd.concat([mult_df, df], axis = 1, join = 'outer', sort = True)
        print(mult_df)
        
    mult_df = mult_df.dropna(axis=0, how='any')
    return mult_df

def get_IV(IVlist, trace_id, timestart, timeend, save_name, skipNaN = True, saveCSV = False):
    '''
    Get IV data from PI System and parse it one dataframe per trace
    
    Parameters:
        IVlist: list of str, tags for current and voltage
        trace_id: str, tag for trace_id
        timestart: str, datetime format of start time
        timeend: str, datetime format of end time
        skipNan: optional boolean, if True, removes dataframes with NaN values
        
    Returns:
        df_list: list of databases
    '''
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

    for i in range(len(df_list)):
        df_list[i].to_csv(save_name + str(i) + '.csv')
    
    return df_list

def save_IV(df_list, saveName):
    '''
    Save all dataframes in multiple csv files
    
    Parameters:
        df_list: dataframe
        saveName: str, name that csv files will be saved as
    '''
    for i in range(len(df_list)):
        df_list[i].to_csv(saveName + str(i) + '.csv')

def get_IV_csv(IVlist, saveName, trace_id, timestart, timeend, skipNaN = True):
    '''
    Gets the list of dataframes of IV data and saves the dataframes 
    in their own csv files
    
    Parameters:
        IVlist: list of str, tags for current and voltage
        saveName: str, the name of the saved csv files
        trace_id: str, tag for trace_id
        timestart: str, datetime format of start time
        timeend: str, datetime format of end time
        skipNan: optional boolean, if True, removes dataframes with NaN values
        
    Returns:
        df_list: list of databases
    '''
    df_list = get_IV(IVlist, trace_id, timestart, timeend, skipNaN)
    save_IV(df_list, saveName)
    return df_list
        
def rename__cols(df):
    new_colnames = []
    for col in df.columns:
        new_colnames.append((col.split(".")[0]))
    df.columns = new_colnames
    return df

def Store_Vals(df, valuecol, pointname):
    '''
    Store values into the PI System
    
    Parameters:
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

def get_mysql_data(string, table):
    '''
    Query mySQL server and return dataframe
    
    Parameters:
        string: str, mySQL string name (i.e. 8157S1)
        table: str, mySQL table name (i.e. iv.trace)
        
    Return:
        df_trace: dataframe returned with information from query
    '''
    # query the trace_id in mySQL
    trace_id_query = "SELECT CAST(datetime AS datetime), CAST(groupchannel_name as BINARY), trace_id FROM " + f'"{table}"' + "WHERE groupchannel_name = " +f'"{string}"'  + "ORDER BY datetime DESC;"
    traceid_list = run_mysql(trace_id_query, host, port, database, username, password)
    
    # Create dataframe of datetime, groupchannel_name, and trace_id
    df_trace = pd.DataFrame(traceid_list, columns=['datetime', 'group', 'trace_id'])
    return df_trace


if __name__ == "__main__":   
    results = get_any_tag_values(['current', 'voltage', '8157_UCF.UCF_Inverter_1.CB_1.S_1.Isc'], timestart = '10/05/2018 9:00:00 AM', timeend = '10/05/2018 4:00:00 PM', element = '8157_S1', save_csv = False)
    
    I = results[0]
    V = results[1]
    Isc = results[2]
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    