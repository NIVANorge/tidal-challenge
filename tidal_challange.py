## Date:     29.04.2018 
## Function: This script calculates chart datum using depth measurements from the field and tidal information from API for water level data.
##           Chart datum is added as an extra column to the input file containing measurements and saved in xlsx file for an easy access by the researchers.
##           In addition, the complete information retrieved from API is stored in the output XML for further processing if needed.
## Usage:    python tidal_challange.py filename.xlsx 
## Output:   filename_out.xlsx 
##           filename_out.xml 
##
## ----------------------------------------------------------------     
## -----------Import relevant modules------------------------------
## ----------------------------------------------------------------    
import sys
import pandas as pd             ## for data handling
import requests as rq           ## for retrieving info from API   
try:                            ## for manipulating XML tree (try C version if available because its faster)
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree
from datetime import datetime   ## for formating date and time
from datetime import timedelta
from datetime import time
## ----------------------------------------------------------------                                                                                                               
## -----------Constants--------------------------------------------                                                                                                                ## ----------------------------------------------------------------  
## Depth measurements are in [m] (actually this information is not provided anywhere but it is assumed they are)  
## while tidal info extracted from API is in [cm]
## Calculated chart datum is in [m]
## In pratice information on units should be extracted from excel (NA for now) and from api and not hardcoded
unit        = 100 
## ----------------------------------------------------------------
## -----------Support functions -----------------------------------
## ----------------------------------------------------------------
def repl(x,a,b):
    try:
        return x.replace(a, b)
    except AttributeError:
        return x

def format_input(df):
## Brute force, ok for now, but a more generic version should handle exceptions in a better way
    try:
        df=df.replace(to_replace="<Null>",value="nan")
        df['GPS Longitude'] = df['GPS Longitude'].apply(lambda x: repl(x,',','.'))
        df['GPS Longitude'] = df['GPS Longitude'].apply(pd.to_numeric,errors='coerce')
        df['GPS Latitude']  = df['GPS Latitude'].apply(lambda x: repl(x,',','.'))
        df['GPS Latitude']  = df['GPS Latitude'].apply(pd.to_numeric,errors='coerce')
        df['depth']         = df['depth'].apply(lambda x: repl(x,',','.'))
        df['depth']         = df['depth'].apply(pd.to_numeric,errors='coerce')
        df['Date']          = df['Date'].apply(lambda x: repl(x,'.',' '))
        df.dropna(axis=0, how='any',subset=['Date','Time','GPS Longitude','GPS Latitude','depth'],inplace=True)
        return df
    except:
        exit('Formating of input data failed')

def format_datetime(da,ti,lower=True,inter=5):
## Handling of different input data formats could be added instead of throwing exception
    half_int=timedelta(minutes=inter)
    try:
        dati = datetime.strptime("{} {}".format(da, ti), '%d %m %Y %H:%M')                                 
        if lower==True :                             
            dati_ = (dati - half_int).strftime("%Y-%m-%dT%H:%M")
        else:
            dati_ = (dati + half_int).strftime("%Y-%m-%dT%H:%M")
        return dati_    
    except ValueError:
        print("Error: Point ",index, " Time: ", row['Time'], " Date: ", row['Date'], " has wrong format")
        print("Info: Required time format: H:M, required date format: dd.mm.yy")
        if lower==True :
            dati_ = "1000-00-00T00:00"
        else:
            dati_ = "1000-00-00T00:10"
        return dati_    
def save_xml(xml_et, filename):
    if xml_et is not None:
        try:
            tree=ElementTree(xml_et)
            tree.write(filename)
        except IOError as er:
            print(str(er))
    else:
        print("No valid XML tree")

def read_xlsx(input_file):
    try:
        df = pd.read_excel(input_file)
        return df
    except IOError as er:
        print(str(er))
        sys.exit('Error: Reading input file')

def save_xlsx(df, filename):
    try:
        out = pd.ExcelWriter(filename)
        df.to_excel(out, sheet_name='Sheet1')
        out.save()
    except IOError as er:
        print(str(er))
        sys.exit('Error: Writing Excel file')

## ---------------------------------------------------------------- 
## -----------Input -----------------------------------------------
## ---------------------------------------------------------------- 
if len(sys.argv) < 2:
    print("Usage: python tidal_challange.py filename.xlsx")
    sys.exit('Error: too few arguments, please specify an input filename')

input_file = str(sys.argv[1])        ## input file name
df         = read_xlsx(input_file)   ## read in data from excel and store in a dataframe
df         = format_input(df)        ## Format columns in convenient way for further processing

## ----------------------------------------------------------------                                                                                                            
## -----------Main body-------------------------------------------- 
## ----------------------------------------------------------------         
xml_ET      = None                   ## define XML output tree
chart_datum = pd.Series()            ## array of chart datum
URL         = "http://api.sehavniva.no/tideapi.php"  ## API URL
daty        = 'PRE'
## Loop over measurements. For each measurement point (date, time) extract tidal information by calling API for water level
## Store tidal information in a series indexed by measurements                                                             
## If measured point lies exactly in the middle of the time interval, there are two tidal points returned by API.                                                                    
## For now the later point is used. Other options (such e.g. linear interpolation/average) can be added depending on requested precision              

for index, row in df.iterrows():

## Create dictionary of input parameters for API 
    fromtime = format_datetime(row['Date'],row['Time'])
    totime = format_datetime(row['Date'],row['Time'],lower=False)
    inputs={'tide_request':'locationdata','lat':row['GPS Latitude'],'lon':row['GPS Longitude'],'datatype':daty,'refcode':'CD','fromtime':fromtime,'totime':totime,'interval':'10'}

## Get waterlevel data by calling the API   
    try:
        rs = rq.get(URL,params=inputs)
    except rq.exceptions.RequestException as er:  
        print(str(er))
        print("Error: Retrieving data failed ")
        chart_datum.at[index] = float('nan')
        continue

## Create XML tree from API output string
    root     = ET.fromstring(rs.content)
## Store tidal values in chart_datum series 
    nPoints  = len(root.findall('.//waterlevel'))
    if nPoints < 1:
        chart_datum.at[index]=float('nan')
        print ("No tidal information for point: ",index, " Time: ", row['Time'], " date: ", row['Date'], "Latitude ", row['GPS Latitude'], "Longitude ", row['GPS Longitude'])
    else:
        for child in root.iter('waterlevel'):
            chart_datum.at[index]=child.attrib['value']

## Store API output in an xml tree 
    for child in root.iter('tide'):
        if xml_ET is None:
            xml_ET = root 
        else:
            xml_ET.extend(child) 

## ----------------------------------------------------------------     
## -----------Store results----------------------------------------  
## ----------------------------------------------------------------     

## Derive output file names 
## Ok for now but a generic version should allow for different input and output paths as well as a more robust deriviation of output filenames. 
name=input_file[:-5]
output_file1=name+"_out.xml"
output_file2=name+"_out.xlsx"

## Check wether there exist at least one valid waterlevel tidal point                                                                                                             
if(chart_datum.isnull().all()):
    sys.exit('Error: no waterlavel data')

## Save API output in xml file
save_xml(xml_ET, output_file1)

## Format tidal information, calculate chart datum and add it as an extra column to the existing data
## Note: measurements are not re-indexed after removing Nulls from the input
chart_datum     = chart_datum.apply(pd.to_numeric)            ## convert string to numerical values
dpth            = pd.Series(df['depth'])                      ## convert depth info to a series
chart_datum     = -chart_datum/unit + dpth                    ## calculate chart datum
df['Date']      = df['Date'].apply(lambda x: repl(x,' ','.')) ## go back to the original date format
df.insert(6,'chart_datum',chart_datum)                        ## insert the chart datum column

## Save the result in an excel file
save_xlsx(df, output_file2)


