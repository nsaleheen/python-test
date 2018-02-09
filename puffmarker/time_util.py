# Shooth function: moving average of n values
# US/Alaska
# US/Aleutian
# US/Arizona
# US/Central
# US/East-Indiana
# US/Eastern
# US/Hawaii
# US/Indiana-Starke
# US/Michigan
# US/Mountain
# US/Pacific
# US/Pacific-New
# US/Samoa
import datetime, time
import pytz

tz = pytz.timezone('US/Central')
print(tz)

# unix time to '2017-11-01 15:52:00'
def unixtime_to_datetime(timestamp):
    dt = datetime.datetime.fromtimestamp(timestamp, tz).strftime('%Y-%m-%d %H:%M:%S')
    return dt
    

#unix time to  '2017-11-01 15:52:00' -> '2017-11-01'
def unixtime_to_date(timestamp):
    dt = unixtime_to_datetime(timestamp)
    return dt.split(' ')[0]

#unix time to  '2017-11-01 15:52:00' -> '15:52:00'
def unixtime_to_time(timestamp):
    dt = unixtime_to_datetime(timestamp)
    return dt.split(' ')[1]

#unix time to '15*52' in minutes
def unixtime_to_timeOfDay(timestamp):    
    tm = unixtime_to_time(timestamp)
    toks = tm.split(':')
    h = int(toks[0])
    m = int(toks[1])
    timeOfday = h*60 + m    
    return timeOfday

ut = 1386181800

print(unixtime_to_datetime(ut))
print(unixtime_to_date(ut))
print(unixtime_to_time(ut))
print(unixtime_to_timeOfDay(ut))


