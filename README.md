# PI-Python
Python code targeting and using PI data

OSI_loadfloat.py - contains functions that focus on interactions with the PI System by connecting to the data archive directly

Pithon_Loadfloat_Systems.py - contains functions for getting information from the PI System by connecting to the AF Client

interfacePithon.py - active code running on the Interface machine which grabs data from mySQL server, parses the data, adds a datetime timestamp, and stores into the PI System
