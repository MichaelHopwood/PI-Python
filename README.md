# PI-Python
Python code targeting and using PI data

`OSI_loadfloat.py` - contains functions that focus on interactions with the PI System by connecting to the data archive directly

`Pithon_Loadfloat_Systems.py` - contains functions for getting information from the PI System by connecting to the AF Client

`interfacePithon.py` - active code running on the Interface machine which grabs data from mySQL server, parses the data, adds a datetime timestamp, and stores into the PI System

### OSI_loadfloat

| Function | Description | Inspiration |
| ------ | ----------- | --------- |
| Summarize_PI_Data   | Grabs data from PI by querying values at periodic timestamps and using interpolated values if no real value is present. | __[RobMulla](https://github.com/RobMulla/PIMachineLearning/blob/master/Code/OSI.py)__
| Summarize_Multi_PIData | Iterates Summarize_PI_Data to create a larger dataframe with more queried tags. | __[RobMulla](https://github.com/RobMulla/PIMachineLearning/blob/master/Code/OSI.py)__
| get_tag_values  | Grab actual values from PI System within queried time range | `Eric Schneller and Siyu Guo`
| get_mult_values  | Query list of pi tags and add to dataframe. *Currently, values must have the same timestamp* | |  
| get_IV  | Grab all IV data within inputted time range. Returns list of Dataframes | |  
| save_IV  | Save all dataframes in multiple csv files | | 
| get_IV_csv  | Combines `get_IV` and `save_IV` into one function| | 
| Store_Vals  | Store values from dataframe into PI System| | 


### Pithon_Loadfloat_Systems

| Function | Description |
| ------ | ----------- |
| data   | path to data files to supply the data that will be passed into templates. |
| engine | engine to be used for processing templates. Handlebars is the default. |
| ext    | extension to be used for dest files. |
