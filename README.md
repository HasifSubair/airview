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
The datasets are combined and stored in a de-normalized format as parquet files in S3 or HDFS any file system.


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


