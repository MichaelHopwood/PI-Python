# -*- coding: utf-8 -*-
"""
Created on Thu Sep 20 02:03:39 2018

@author: Michael Hopwood
"""

import sys
import clr
import numpy as np
import pandas as pd

sys.path.append("C:\\Program Files (x86)\\PIPC\\AF\\PublicAssemblies\\4.0\\")
clr.AddReference('OSIsoft.AFSDK')

from OSIsoft.AF import *
from OSIsoft.AF.Asset import *
from OSIsoft.AF.Time import *
from System import *

pd.options.display.max_rows
pd.options.display.max_columns

def connect_server(server):
    piSystems = PISystems()
    piSystem = piSystems[server]
    databases = piSystem.Databases
    data = databases.GetEnumerator()
    for i in data:
        if i.Name == "PVStations":
            return i
        else:
            continue
    return None

def get_table(table_name, save_location, database = "PVStations", server = "net1552.net.ucf.edu"):

    '''
    This function provides a way to receive tables that reside in PI System Explorer.
    A means of connecting to the server is built into this function.
    
    Parameters
    -----------
    table_name : string
        Name of desired table 
        
    save_location : string
        Address where you want the table csv to be saved
        
    database : string, default "PVStations"
        Database where the table resides
    
    server : string, default "net1552.net.ucf.edu"
        Full server name
    
    
    Returns
    -----------
    table : DataFrame
    
    '''

    # Initialize PISystems
    piSystems = PISystems()
    # Choose a server from PISystems
    piSystem = piSystems[server]
    # Get all databases
    databases = piSystem.Databases
    # Parse list of database
    data = databases.GetEnumerator() #listdatabases . Methods()

    # Choose database
    for i in data:
        if i.Name == database:
            break
        else:
            continue
    
    # Get tables in database
    aftables = i.Tables
    # Parse list of tables
    tables = aftables.GetEnumerator()

    # Choose table
    for j in tables:
        if j.Name == table_name:
            break
        else:
            continue

    # Initialize lists
    lst = []
    rowlst = []
    maxi = 0
    
    # Get number of columns
    for col in j.Table.Columns:
        maxi+=1

    # Create list of lists that has data
    for row in j.Table.Rows:
        i = 0
        rowlst = []
        for col in j.Table.Columns:
            i += 1
            point = row[col]
            rowlst.append(point)
            if i == maxi:
                lst.append(rowlst)
            
    # Convert list of lists to dataframe
    table_final = pd.DataFrame.from_records(lst)
    print(table_final)
    
    # Save dataframe as CSV
    table_final.to_csv(save_location)
    
    return table_final

def get_value(path, database, start_time, end_time, interval):
    attribute = AFAttribute.FindAttribute(path, database)
    if attribute is not None:
        start_time = AFTime(start_time)
        end_time = AFTime(end_time)
        time_range = AFTimeRange(start_time, end_time)
        values = attribute.Data.RecordedValues(time_range, 0, None, None, True, 0)
        for value in values:
            print("Value {0}, Timestamp {1}".format(value.Value, value.Timestamp))
            
if __name__ == "__main__":
    database = connect_server('net1552.net.ucf.edu')
    if database is not None:
        get_table("NIST_Insitu_IV", 'table_final_test.csv')
    else:
        print("Connection to database is prohibited")