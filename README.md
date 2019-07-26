# AirView

## Introduction

The U.S. Department of Transportation's (DOT) Bureau of Transportation Statistics (BTS) tracks the on-time performance 
of domestic flights operated by large air carriers. Summary information on the number of on-time, delayed, canceled and 
diverted flights appears in DOT's monthly Air Travel Consumer Report, published about 30 days after the month's end, as 
well as in summary tables posted on this website. Summary statistics and raw data are made available to the public at 
the time the Air Travel Consumer Report is released.

Source data can be found [here](http://stat-computing.org/dataexpo/2009/the-data.html) or from [here](https://www.transtats.bts.gov/OT_Delay/OT_DelayCause1.asp)

Supplemental data for airport and carrier can be found [here](http://stat-computing.org/dataexpo/2009/supplemental-data.html)

AirView project enhances the flight data by adding information on the Airports, Aircrafts and the Carrier information. 
The datasets are combined and stored in a de-normalized format as parquet files in S3 or HDFS any file system. The final
dataset can help finding answers to some of the common questions.
1. The reason for a flight being late or cancelled.
2. Which airlines has more on-time arrivals for a particular route.
3. Delay causes by year.
4. Prediction on flight being delayed/cancelled on a particular day due to weather.
5. How many flights were really delayed by weather?



## Data Sets

### Flights on-time performance data

Carriers that have 0.5 percent of total domestic scheduled-service passenger revenue must report on-time data and the 
causes of delay. Since June 2003, the airlines that report on-time data also report the causes of delays and cancellations to the Bureau 
of Transportation Statistics. Reported causes of delay are available from June 2003 to the most recent month. 

The airlines report the causes of delay in broad categories that were created by the Air Carrier On-Time Reporting 
Advisory Committee.  The categories are Air Carrier, National Aviation System, Weather, Late-Arriving Aircraft and 
Security.  The causes of cancellation are the same, except there is no late-arriving aircraft category.

**How are these delay categories defined?**

* **Air Carrier:** The cause of the cancellation or delay was due to circumstances within the airline's control (e.g. 
maintenance or crew problems, aircraft cleaning, baggage loading, fueling, etc.).
* **Extreme Weather:** Significant meteorological conditions (actual or forecasted) that, in the judgment of the 
carrier, delays or prevents the operation of a flight such as tornado, blizzard or hurricane.
* **National Aviation System (NAS):** Delays and cancellations attributable to the national aviation system that refer 
to a broad set of conditions, such as non-extreme weather conditions, airport operations, heavy traffic volume, and air 
traffic control.
* **Late-arriving aircraft:** A previous flight with same aircraft arrived late, causing the present flight to depart 
late.
* **Security:** Delays or cancellations caused by evacuation of a terminal or concourse, re-boarding of aircraft because
 of security breach, inoperative screening equipment and/or long lines in excess of 29 minutes at screening areas.


| Columns | Data Type | Description | Nullable |
| ------------- |:-------------|:-------------|:-------------|
|Year | Integer | 1987-2008| False |
|Month | Integer | 1-12| False |
|DayofMonth | Integer | 1-31 | False
|DayOfWeek | Integer | 1 (Monday) - 7 (Sunday)| False |
|DepTime |Integer | actual departure time (local, hhmm)| False |
|CRSDepTime | Integer | scheduled departure time (local, hhmm)| False |
|ArrTime | Integer | actual arrival time (local, hhmm)| False |
|CRSArrTime | Integer | scheduled arrival time (local, hhmm)| False |
|UniqueCarrier | String | unique carrier code| False |
|FlightNum | Integer | flight number| False |
|TailNum | String | plane tail number| False |
|ActualElapsedTime | Integer| in minutes| False |
|CRSElapsedTime | Integer | in minutes| False |
|AirTime | Integer | in minutes| False |
|ArrDelay | Integer | arrival delay, in minutes| False |
|DepDelay | Integer | departure delay, in minutes| False |
|Origin | String | origin IATA airport code| False |
|Dest | String | destination IATA airport code| False |
|Distance | Integer | in miles| False |
|TaxiIn | Integer | taxi in time, in minutes| False |
|TaxiOut | Integer | taxi out time in minutes| False |
|Cancelled | Bit | 1 = yes, 0 = no | False |
|CancellationCode | String | reason for cancellation (A = carrier, B = weather, C = NAS, D = security)| True |
|Diverted | Bit | 1 = yes, 0 = no | False
|CarrierDelay | String | in minutes| True |
|WeatherDelay | String | in minutes| True |
|NASDelay | String |in minutes| True |
|SecurityDelay | String | in minutes| True |
|LateAircraftDelay | String | in minutes| True |

### Airports

Describes the locations of US airports. This majority of this data comes from the FAA, but a few extra airports 
(mainly military bases and US protectorates) were collected from other web sources by Ryan Hafen and Hadley Wickham.

| Columns | Data Type | Description | Nullable |
| ------------- |:-------------|:-------------|:-------------|
| iata | String | The international airport abbreviation code | False |
| airport | String | Name of the airport | False |
| city | String | City in which airport is located | False|
| state | String | State in which airport is located | False |
| country | String | Country in which airport is located | False |
| lat | Float | Latitude of the airport| False |
| long |Float | Longitude of the airport | False |

### Carriers

Listing of carrier codes with full names

| Columns | Data Type | Description | Nullable |
| ------------- |:-------------|:-------------|:-------------|
| Code | String | IATA Carrier code | False |
| Description | String | Name of the carrier | False |

### Aircraft

Information about planes by tail numbers.

| Columns | Data Type | Description | Nullable |
| ------------- |:-------------|:-------------|:-------------|
| tailnum | String | Aircraft tail number | False |
| type | String | Type of aircraft | True |
| manufacturer | String | Name of the manufacturer | True |
| issue_date | Date | Date of aircraft incorporated to service | True |
| model | String | Aircraft model | True |
| status | String | Status of the aircraft | True |
| aircraft_type | String | Type of aircraft | True |
| engine_type | String | Type of Engine | True |
| year | Integer | Year of incorporation | True |

### Data Cleaning

1. **Adding timestamp field to flights data**

Flight data year, month, day and time are separated out in separate columns. And the time column is of the format 
'HHmm' as an integer. It will be very difficult to interpret in an analytics table. As part of the cleaning process, we 
combine 'year', 'month', 'day' and 'time' after adding ':' in between hour and minutes and create a timestamp value. The
value is updated to the same columns names. The affected column includes 'CRSDepTime', 'CRSArrTime', 'DepTime' and 
'ArrTime'.

2. **Correcting Actual Times**

Creating the timestamp column by combining the year, month, day and time causes error in actual departure and arrival 
times, which were initially integer values.
For example: Let's a flight is scheduled to depart 5 minutes past midnight, but left 14 minutes early.

| Year | Month | Day | ScheduledDeparture | DepartTimestampValue | ActualDeparture | ActualDepTimestampValue |DepDelay
| ------------- |:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|:-------------|
| 2019 | 1 | 1 | 0005 | 2019-01-01 00:05 | 2351 | 2019-01-01 23:51 | -14

Now we see that DepartTimestampValue and ActualDepTimestampValue is showing a delay of almost 24 hours even though the 
flight left early. To prevent data corruption from these edge cases. ActualDeparture time calculated by adding the 
DepDelay in minutes from the DepartTimestampValue.

In the case above, when we add '-14' minutes from DepartTimestampValue '2018-10-12 00:05', we get the correct 
ActualDepTimestampValue of '2018-10-11 23:54'

### AirView Data Model

The airview data model is as given below. This is achived by de-normalizing the flight data, by adding the origin and 
destination airport info as well as carrier name. The aircraft information is added by joining on 'tailnum' column. 
'DayOfMonth' and 'DayOfWeek' columns are dropped as they don't add to the data anymore. 'Year', 'Month' and 
'CarrierCode' are used as the partition columns in spark and the data is stored in parquet format. As parquet files 
works very efficiently with spark.

| Columns | Data Type | Description | Nullable |
| ------------- |:-------------|:-------------|:-------------|
| tailnum | String | Aircraft tail number | False |
| Year | Integer | 1987-2008| False |
| Month | Integer | 1-12| False |
| DepTime |Timestamp | actual departure time (local, hhmm)| False |
| CRSDepTime | Timestamp | scheduled departure time (local, hhmm)| False |
| ArrTime | Timestamp | actual arrival time (local, hhmm)| False |
| CRSArrTime | Timestamp | scheduled arrival time (local, hhmm)| False |
| CarrierCode | String | IATA Carrier code | False |
| FlightNum | Integer | flight number| False |
| ActualElapsedTime | Integer| in minutes| False |
| CRSElapsedTime | Integer | in minutes| False |
| AirTime | Integer | in minutes| False |
| ArrDelay | Integer | arrival delay, in minutes| False |
| DepDelay | Integer | departure delay, in minutes| False |
| OriginIATACode | String | origin IATA airport code| False |
| DestinationIATACode | String | destination IATA airport code| False |
| Distance | Integer | in miles| False |
| TaxiIn | Integer | taxi in time, in minutes| False |
| TaxiOut | Integer | taxi out time in minutes| False |
| Cancelled | Bit | 1 = yes, 0 = no | False |
| CancellationCode | String | reason for cancellation (A = carrier, B = weather, C = NAS, D = security)| True |
| Diverted | Bit | 1 = yes, 0 = no | False
| CarrierDelay | String | in minutes| True |
| WeatherDelay | String | in minutes| True |
| NASDelay | String |in minutes| True |
| SecurityDelay | String | in minutes| True |
| LateAircraftDelay | String | in minutes| True |
| AircraftManufacturer | String | Aircraft manufacturer name | True |
| AircraftIssueDate | Date | Aircraft in service from date| True | 
| AircraftModel | String | Aircraft model | True || AircraftType | String | Aircraft type | True |           
| EngineType | String | Aircraft engine type | True |
| OriginAirport | String | Origin airport name | True |
| OriginCity | String | Origin airport city | True |
| OriginState | String | Origin airport state | True |
| OriginCountry | String | Origin airport country | True |
| OriginLatitude | String | Origin airport latitude | True |
| OriginLongitude | String | Origin airport longitude | True |
| DestinationAirport | String | Destination airport name | True |
| DestinationCity | String | Destination airport city | True |
| DestinationState | String | Destination airport state | True |
| DestinationCountry | String | Destination airport country | True |
| DestinationLatitude | String | Destination airport latitude | True |
| DestinationLongitude | String | Destination airport longitude | True |
| Carrier | String | Name of the Carrier | True |

## Running the application

![](/images/pipeline.png)

*Pipeline for running AirView project*

### Data ingestion
Executor class had 'ingest_data' method, which reads files from the input file/location and writes to the ouput 
location. To execute data ingestion task execute the below command.

`python3 executor.py ingest_data <INPUT_PATH> <INPUT_FORMAT> <OUTPUT_PATH> <OUTPUT_FORMAT> <WRITE_MODE>`
 
For example, for ingesting flight data from CSV format and overwrite the folder in parquet format.

`python3 executor.py ingest_data <INPUT_FILE> csv <OUPUT_FOLDER> parquet overwrite`

*Parameter WRITE_MODE is optional and is defaulted to 'error'.* 

Different write modes are.

1. 'append'     -   if data already exists, contents of the DataFrame are expected to be
                                                appended to existing data.
2. 'overwrite'  -   if data already exists, existing data is expected to be overwritten by
                                                the contents of the DataFrame.
3. 'error'      -   throws an error if data already exists.
4. 'ignore'     -   if data already exists, the save operation is expected to not save the
                                                contents of the DataFrame and to not change the existing data. 
                                                
### Quality Check

Executor class had 'check_quality' method, checks the record count from the source and destination folders. To execute 
data quality check, execute the below command.

`python3 executor.py check_quality <SOURCE_PATH> <SOURCE_FORMAT> <DESTINATION_PATH> <DESTINATION_FORMAT>`


### Clean flight data

`python3 executor.py clean_flight_data <INPUT_PATH> <INPUT_FORMAT> <OUTPUT_PATH> <OUTPUT_FORMAT> <WRITE_MODE>`

*Parameter WRITE_MODE is optional and is defaulted to 'error'.*

### Clean aircraft data

`python3 executor.py clean_aircraft_data <INPUT_PATH> <INPUT_FORMAT> <OUTPUT_PATH> <OUTPUT_FORMAT> <WRITE_MODE>` 

*Parameter WRITE_MODE is optional and is defaulted to 'error'.*

### Enhance flight data

All input data set must be of parquet format.

`python3 executor.py enhance_flight_data <CLEAN_FLIGHT_DATA_PATH> <CLEAN_AIRCRAFT_DATA_PATH> <AIRPORT_DATA_PATH> 
<CARRIER_DATA_PATH> <OUTPUT_FOLDER>`

### Quality Check

`python3 executor.py quality_check <SOURCE_DATA_PATH> <DESTINATION_DATA_PATH>`

### Run query dynamic queries against the files. 

`python3 executor.py run_query <SOURCE_FILE_PATH> <SOURCE_FORMAT> <TEMP_TABLE_NAME> <QUERY> <NUMBER_OF_RECORDS>`



## Sample queries to be run against the files.

1. Sum of all delays by the carrier.

`SELECT Carrier, SUM(CarrierDelay), SUM(WeatherDelay), SUM(NASDelay), SUM(SecurityDelay), SUM(LateAircraftDelay) 
FROM flights GROUP BY(Carrier)`˚

2. Sum of all delays for all carrier year wise.

`SELECT Carrier, Year, SUM(CarrierDelay), SUM(WeatherDelay), SUM(NASDelay), SUM(SecurityDelay), SUM(LateAircraftDelay) 
FROM flights 
GROUP BY Carrier, Year ORDER BY Carrier`˚

3. Airport with the most National Aviation System(NAS) delays, which includes airport operations, heavy traffic volume, 
and air traffic control.
`SELECT DestinationAirport, DestinationCity, DestinationState, SUM(NASDelay) as NASDelay 
FROM flights 
GROUP BY DestinationAirport, DestinationCity, DestinationState 
ORDER BY NASDelay`

4. Number of flights with LateAircraftDelay and yet arrived on time or before time.
`SELECT * 
FROM flights
where LateAircraftDelay > 0 and ArrDelay <= 0`


## Project Write ups

**What's the goal?**

The goal is create a big analytical database of flight records, on which queries for different analytical use-cases can 
be run. The database should involve flights, aircrafts, airports and carriers information.

**What queries will you want to run?** 

Some of the queries are as below.
1. Sum of all delays by the carrier.

`SELECT Carrier, SUM(CarrierDelay), SUM(WeatherDelay), SUM(NASDelay), SUM(SecurityDelay), SUM(LateAircraftDelay) 
FROM flights GROUP BY(Carrier)`˚

2. Sum of all delays for all carrier year wise.

`SELECT Carrier, Year, SUM(CarrierDelay), SUM(WeatherDelay), SUM(NASDelay), SUM(SecurityDelay), SUM(LateAircraftDelay) 
FROM flights 
GROUP BY Carrier, Year ORDER BY Carrier`˚

3. Airport with the most National Aviation System(NAS) delays, which includes airport operations, heavy traffic volume, 
and air traffic control.
`SELECT DestinationAirport, DestinationCity, DestinationState, SUM(NASDelay) as NASDelay 
FROM flights 
GROUP BY DestinationAirport, DestinationCity, DestinationState 
ORDER BY NASDelay`

4. Number of flights with LateAircraftDelay and yet arrived on time or before time.
`SELECT * 
FROM flights
where LateAircraftDelay > 0 and ArrDelay <= 0`

**How would Spark or Airflow be incorporated?**

Project utilizes Spark heavily as it will involve huge volume of data. The flight database for year 2008 has more than 
7 million records. We can use Airflow with spark submit operator [here](https://github.com/apache/airflow/blob/master/airflow/contrib/operators/spark_submit_operator.py)
to run the spark jobs in the pipeline as shown in the figure above.

**Why did you choose the model you chose?**

Commercial flight records for United States alone for the year of 2008 had over 7 million records. Scaling the program 
to incorporate the flight details across the globe for about 10 years will increase the data volume by over 100s or even
1000s of times. Processing this volume of records in a Relational Database will be slow and won't cost effective. Spark
with its in-memory and distributed processing capability can execute the queries faster, efficeiently and will be more 
cost effective. Spark works best with parquet file format and de-normalizing the data will prevents joins in spark query
and will give better performance.


**Clearly state the rationale for the choice of tools and technologies for the project**

*Spark:* for distributed processing of big data.

*AirFlow* for scheduling and building pipeline of different tasks.

*Parquet file format* is best suited for spark's distributed processing.

*S3* for storing large volume of data in the cloud.

*EMR* for running spark jobs on AWS.   
 
**Document the steps of the process.**

1. Flights, airports, aircraft and carrier data is ingested into S3.
2. Quality check to be performed for flights, airports, aircrafts and carrier data.
3. Clean flights and aircrafts data.
4. Join airports, aircrafts and carriers data with flight data.

**Propose how often the data should be updated and why**

The data can be ingested in real-time as a stream with spark streaming. This will help in real-time dashboards.
For example:
1. If flight is running late then, the LateAircraftDelay for a particular carrier at that airport will increase 
accordingly.
2. If there is 'NASDelay' in particular airport then, we can expect all aircraft to that airport to be delayed for some
time.

If the data cannot be loaded as streams then it data should can be loaded on an hourly batch job. This keeps the batch
size relative small and an hour latency will be tolerable for most machine learning use-cases.  

**If the data was increased by 100x.**

Since the data is source data is loaded to S3, S3 can handle scale and accomodate the data. The data can be further 
partitioned based on the use-cases being run to improve the performance of the query.

**If the pipelines were run on a daily basis by 7am.**

We can schedule a Airflow DAG to run daily at 7am.

**If the database needed to be accessed by 100+ people.**

We won't be able to run 100s of spark query within the cluster. When such requirement arises, we should start storing 
output of analytical data into different tables and users can read data from these tables.

 
 

