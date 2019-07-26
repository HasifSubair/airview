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
| Columns | Description | 
| ------------- |:-------------|
|Year | 1987-2008|
|Month | 1-12|
|DayofMonth | 1-31|
|DayOfWeek | 1 (Monday) - 7 (Sunday)|
|epTime | actual departure time (local, hhmm)|
|CRSDepTime | scheduled departure time (local, hhmm)|
|rrTime | actual arrival time (local, hhmm)|
|CRSArrTime | scheduled arrival time (local, hhmm)|
|UniqueCarrier | unique carrier code|
|FlightNum | flight number|
|TailNum | plane tail number|
|ActualElapsedTime | in minutes|
|CRSElapsedTime | in minutes|
|AirTime | in minutes|
|ArrDelay | arrival delay, in minutes|
|DepDelay | departure delay, in minutes|
|Origin | origin IATA airport code|
|Dest | destination IATA airport code|
|Distance | in miles|
|TaxiIn | taxi in time, in minutes|
|TaxiOut | taxi out time in minutes|
|Cancelled | was the flight cancelled?|
|CancellationCode | reason for cancellation (A = carrier, B = weather, C = NAS, D = security)|
|iverted | 1 = yes, 0 = no|
|CarrierDelay | in minutes|
|eatherDelay | in minutes|
|ASDelay | in minutes|
|SecurityDelay | in minutes|
|LateAircraftDelay | in minutes|




The dataset can be later combined with weather and aircraft repair data to predict aircraft repair cycles.