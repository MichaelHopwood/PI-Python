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

def get_any_tag_values(parameters, timestart, timeend, element = '8157_S1', save_csv = False, save_name = 'defaultName', save_location = None, parse = False, combine_iv_data = False):
    '''
    Get values for all tags from any source. 
    
    PARAMETERS:
        parameters: list of str, data name
            PI System data: tag data archive name
            mySQL data: IV trace data - input 'current' or 'voltage' for IV trace data
        timestart: str, datetime of intended start of query
            Used for PI System data query
        timeend: str, datetime of intended end of query
            Used for PI System data query
        element: str, specify which apparatus you want the data for
                 If you want data for everything, use 'any'
                 Current Possibilities: '8157_S1', '8157_S2', 'any'
        save_csv: bool, state whether you want the dataframes saved as csv files
                If True, files will be saved
        save_name: str, csv file name
                Name appended with the parameter's name and the index in the parameter list
                i.e. defaultName_current_1.csv
        save_location: str, specify location where csv is saved
                if None, csv saved in directory where python file is located
                i.e. 'C:\Documents' will save the csv in your Documents folder
        parse: bool
                if True, parse the IV data into pairs instead of lists
        combine_iv_data: bool
                if True, place current and voltage data in the same dataframe

    If trace data (IV_I, IV_V) is included in query, the 'element' parameter must be included in the function call.
            
            To PARSE trace data out of their lists, set parse to True
            To COMBINE trace data into one dataframe, set combine_iv_data to True
            
    
    '''
    
    groups = []
    results = []
    
    host = 'INPUT_IP_HERE'
    port = 'INPUT_PORT_HERE'
    username = 'INPUT_USERNAME_HERE'
    password = 'INPUT_PASSWORD_HERE'
    database = 'INPUT_DATABASE_HERE'
    
    # To be used later when determining which table should be queried
    trace_table_parameter_list = ['trace_id', 'datetime', 'groupchannel', 'groupchannel_name', 'trace_type', 'num_points', 'current', 'voltage', 'irradiance', 'aux', 'isc', 'voc', 'pmax', 'ipmax', 'vpmax', 'ff', 'avg_irradiance', 'avg_aux', 'Rch', 'Rsh', 'Rsh_r2', 'Rs', 'Rs_r2', 'additional_data', 'exclude', 'extract', 'analysis_datetime', 'analysis_code_version', 'analysis_classification', 'analysis_user_verification', 'analysis_i', 'analysis_v', 'analysis_isc', 'analysis_voc', 'analysis_pmax', 'analysis_ipmax', 'analysis_vpmax', 'analysis_ff']
    
    mysql_timestart = datetime.datetime.strptime(timestart, '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S')
    mysql_timeend = datetime.datetime.strptime(timeend, '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S')
    for i in range(len(parameters)):
        
        # access the mySQL database if the user queries IV_I or IV_V data
        if(parameters[i] == 'current' or parameters[i] == 'voltage'):
            # append ID to group for later use 
            # ID = 1 means IV data
            groups.append(1)
            if element == 'any':
                query = "SELECT datetime, groupchannel_name, " + parameters[i] + " FROM iv.trace WHERE (datetime BETWEEN " + f'"{mysql_timestart}"' + " AND " + f'"{mysql_timeend}"' + ");"
            else:
                query = "SELECT datetime, groupchannel_name, " + parameters[i] + " FROM iv.trace WHERE (datetime BETWEEN " + f'"{mysql_timestart}"' + " AND " + f'"{mysql_timeend}"' + ") AND (groupchannel_name = " + f'"{element}"' + ");"
            data = run_mysql(query, host, port, database, username, password)
            df = pd.DataFrame(data, columns=['datetime', 'group', parameters[i]])
            df.set_index(df['datetime'], inplace = True)
            
            # parse data if true by using the reformat_IV function
            if parse is True:
                df = reformat_IV(df, parameters[i])
                
            # remove datetime column, datetime is saved in index
            df.drop(df.columns[[0]], axis=1, inplace = True)
            
        else:
            # Query data from PI Data Archive
            # Group ID = 0 means non-IV data  
            df = get_tag_values(parameters[i], timestart, timeend)
            groups.append(0)
        
        results.append(df)
        
    if 'voltage' in parameters:
        if 'current' in parameters:
            IV_flag = 1
        else:
            IV_flag = 0
    else:
        IV_flag = 0
    
    if combine_iv_data is True:
        if IV_flag == 1:
            new_results = []
            new_params = []
            
            # keep non-IV data constant
            for i in range(len(groups)):
                if groups[i] == 0:
                    new_results.append(results[i])
                    new_params.append(parameters[i])
                    
            # combine IV data into one dataframe
            iv_df = pd.DataFrame()
            for i in range(len(groups)):
                if groups[i] == 1:
                    iv_df[parameters[i]] = results[i][parameters[i]]
    
            # append IV data to results
            new_results.append(iv_df)
            new_params.append('IV_data')
            
            results = new_results
            parameters = new_params

        else:
            print("'combine_iv_data' parameter is set to True but both components of an IV trace are not included in the data query.\n")
            
    # save csv with save_name and parameter values
    if save_csv is True:
        for i in range(len(results)):
            if save_location is not None:
                results[i].to_csv(save_location + '\\' +  save_name + '-' + parameters[i] + '_' + str(i) + '.csv')
                
            else:
                results[i].to_csv(save_name + '-' + parameters[i] + '_' + str(i) + '.csv')
            
    return results

def reformat_IV(df, parameter):
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

# Decommissioned because IV data is not stored in the PI System anymore
def get_PI_IV(IVlist, trace_id, timestart, timeend, save_name = 'default', skip_nan = True, save_csv = False):
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
            if skip_nan == True:
                badIndexes.append(k)

    for x in range(len(df_list)):
        for y in range(len(badIndexes)):
            if badIndexes[y] == x:
                del df_list[x]
                print("removing element: ", x)
    if save_csv is True:
        for i in range(len(df_list)):
            df_list[i].to_csv(save_name + str(i) + '.csv')
    
    return df_list

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
                

if __name__ == "__main__":   

    # Example of how to use get_any_tag_values function
    results = get_any_tag_values(parameters = ['current', 'voltage', '8157_UCF.UCF_Inverter_1.CB_1.S_1.Ipmax'],
                                 timestart = '10/05/2018 12:05:00 PM',
                                 timeend = '10/05/2018 12:35:00 PM',
                                 element = 'any',
                                 save_csv = True,
                                 save_name = 'test',
                                 save_location = 'Desktop',
                                 parse = False,
                                 combine_iv_data = True)
        
        
