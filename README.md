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

1. **Flight data doesn't have a timestamp field**

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

### Running the application

![](https://github.com/HasifSubair/airview/tree/dev/images/pipeline.png)