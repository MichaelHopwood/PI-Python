# PI-Python
Python code targeting and using PI data

`Pithon_Servers.py` - contains functions that focus on interactions with the PI System by connecting to the data archive directly

`Pithon_Systems.py` - contains functions for getting information from the PI System by connecting to the AF Server

`interfacePithon.py` - active code running on the Interface machine which grabs data from mySQL server, parses the data, adds a datetime timestamp, and stores into the PI System

Osisoft Packages: __[system hierarchy](https://techsupport.osisoft.com/Documentation/PI-AF-SDK/html/eb961f37-282a-43d2-8f8c-f19ce07d9fa8.htm)__

### Pithon Servers
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


### Pithon_Systems
##### Access PI information by connecting to the system 
| Function | Description |
| ------ | ----------- |
| get_table   | Returns dataframe of table saved in PI System. |
| get_value | Grab value of tag in inputted range and interval |

### interfacePithon 
*decomissioned due to lack of development of AFBufferSystem on PI end*

| Function | Description |
| ------ | ----------- |
| get_tag_snapshot   | Retrieved from __[8157_UCF](https://github.com/8157-UCF-JWW/8157UCF/blob/master/P_Functions/Pithon_functions.py)__ and altered to check if PI Point has data  |
| get_tag_values | Same function as in `Pithon_Servers` |
| Store_Vals | Same function as in `Pithon_Servers` with a buffer added on top of it|
| run_mysql | Connect to mySQL database|
| get_mysql_data | Query mySQL data and return into a dataframe|
| reformat_IV | Parse and timestamp a list of tuples so that the data can be formatted to store into the PI System |
| dataQuality | A check of the data quality for mySQL to PI data stream for IV trace data.  Quality is ensured by comparing the length of the IV list with the temporary dataframe AND the stored trace in the PI System  |
| statusPrint | Print status from `dataQuality` in interfaceLogs and write a status of 0 (safe) or 1 (bad) to a tag in the PI System |
| extractTransformLoad | Combine `get_mysql_data`, `reformat_IV`, and `Store_Vals` into one function because it will be iterrated for each tag that is stored |



