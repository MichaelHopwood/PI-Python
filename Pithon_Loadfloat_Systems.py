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

df = pd.DataFrame()
df = pd.read_csv("sampleData.csv")
print(df)

def connect_server():
    piSystems = PISystems()
    piSystem = piSystems["net1552.net.ucf.edu"]
    databases = piSystem.Databases
    data = databases.GetEnumerator()
    for i in data:
        if i.Name == "PVStations":
            return i
        else:
            continue
    return None

def get_table(table_name, database = "PVStations", server = "net1552.net.ucf.edu"):

    '''
    This function provides a way to receive tables that reside in PI System Explorer.
    A means of connecting to the server is built into this function.
    
    Parameters
    -----------
    table_name : string
        Name of desired table 
        
    database : string, default "PVStations"
        Database where the table resides
    
    server : string, default "net1552.net.ucf.edu"
        Full server name
    
    
    Returns
    -----------
    table : DataFrame
    
    '''

    piSystems = PISystems()   # variable = Class()
    piSystem = piSystems[server]   #variable = class[instance]
    databases = piSystem.Databases  #pisystem . properties
    data = databases.GetEnumerator() #listdatabases . Methods()

    for i in data:
        if i.Name == database:
            break
        else:
            continue
    
    aftables = i.Tables
    tables = aftables.GetEnumerator()

    for j in tables:
        if j.Name == table_name:
            break
        else:
            continue

    lst = []
    rowlst = []
    maxi = 0
    
    for col in j.Table.Columns:
        maxi+=1

    for row in j.Table.Rows:
        i = 0
        rowlst = []
        for col in j.Table.Columns:
            i += 1
            point = row[col]
            rowlst.append(point)
            if i == maxi:
                lst.append(rowlst)
            
    
    table_final = pd.DataFrame.from_records(lst)
    print(table_final)
    return 0

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
    database = connect_server()
    if database is not None:
#        interval = AFTimeSpan(0,0,0,1,0)
#        result = get_value("\\8157_UCF_FSEC\\MET1|Direct_Irr_Av" , database, "-10d", "*", interval)
        get_table("ABB details")
    else:
        print("not working")