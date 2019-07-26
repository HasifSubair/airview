# AirView

### Introduction

The U.S. Department of Transportation's (DOT) Bureau of Transportation Statistics (BTS) tracks the on-time performance 
of domestic flights operated by large air carriers. Summary information on the number of on-time, delayed, canceled and 
diverted flights appears in DOT's monthly Air Travel Consumer Report, published about 30 days after the month's end, as 
well as in summary tables posted on this website. Summary statistics and raw data are made available to the public at 
the time the Air Travel Consumer Report is released.

Source data can be found [here](http://stat-computing.org/dataexpo/2009/the-data.html)

Supplemental data for airport and carrier can be found [here](http://stat-computing.org/dataexpo/2009/supplemental-data.html)

Aircraft details can be found [here](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download/)

AirView project enhances the flight data by adding information on the Airports, Aircrafts and the Carrier information. 
The datasets are combined and stored in a de-normalized format as parquet files in S3 or HDFS any file system.


### Data Sets

#### Flights
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




The dataset can be later combined with weather and aircraft repair data to predict aircraft repair cycles.