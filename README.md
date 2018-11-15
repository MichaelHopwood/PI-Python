# PI-Python
Python code targeting and using PI data

`OSI_loadfloat.py` - contains functions that focus on interactions with the PI System by connecting to the data archive directly

`Pithon_Loadfloat_Systems.py` - contains functions for getting information from the PI System by connecting to the AF Client

`interfacePithon.py` - active code running on the Interface machine which grabs data from mySQL server, parses the data, adds a datetime timestamp, and stores into the PI System

Osisoft Packages: __[system hierarchy](https://techsupport.osisoft.com/Documentation/PI-AF-SDK/html/eb961f37-282a-43d2-8f8c-f19ce07d9fa8.htm)__

### OSI_loadfloat
##### Access PI information by connecting to the archive
| Function | Description |
| ------ | ----------- | 
| Summarize_PI_Data   | Grabs data from PI by querying values at periodic timestamps and using interpolated values if no real value is present. | 
| Summarize_Multi_PIData | Iterates `Summarize_PI_Data` to create a larger dataframe with more queried tags. | 
| get_tag_values  | Grab actual values from PI System within queried time range | 
| get_mult_values  | Query list of pi tags and add to dataframe. *Currently, values must have the same timestamp* | |  
| get_IV  | Grab all IV data within inputted time range. Returns list of Dataframes | |  
| save_IV  | Save all dataframes in multiple csv files | | 
| get_IV_csv  | Combines `get_IV` and `save_IV` into one function| | 
| Store_Vals  | Store values from dataframe into PI System| | 


### Pithon_Loadfloat_Systems
##### Access PI information by connecting to the system 
| Function | Description | Inspiration |
| ------ | ----------- | ---------|
| get_table   | Returns dataframe of table saved in PI System. | |
| get_value | Grab value of tag in inputted range and interval |

