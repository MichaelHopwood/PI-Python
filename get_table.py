# -*- coding: utf-8 -*-
"""
Created on Tue Sep 18 10:40:35 2018

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

def get_table(table):
    piSystems = PISystems()
    piSystem = piSystems["net1552.net.ucf.edu"]
    databases = piSystem.Databases
    data = databases.GetEnumerator()
    for i in data:
        if i.Name == "PVStations":
            return i
        else:
            continue
#    afDatabases = AFDatabases()
#    afDatabase = afDatabases["PVStations"]
#    tables = AFDatabase.AFTables
#    return_table = AFTable.Table(table)
#    return return_table
    


#def get_table12(table):
#    
#    tables = AFDatabase.AFTables
#    return_table = AFTable.Table(table)
#    return return_table
##    aftables = AFTables()
##    AFTable = aftables[table]
##    Table_return = Table(table)
#    return Table_return

if __name__ == "__main__":
    database = connect_server_pisystems()
    if database is not None:
        table = get_table("NIST_Insitu_IV")
        print(table)