import happybase
import time as mod_time
from datetime import datetime
import sys

# python hbaseread.py 8 2017-11-10T09:11 2017-11-10T11:22
bus_line = sys.argv[1]
start_time = sys.argv[2]
end_time = sys.argv[3]

start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
start_ts = int((mod_time.mktime(start_time.timetuple())+start_time.microsecond/1e6))

end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M")
end_ts = int((mod_time.mktime(end_time.timetuple())+end_time.microsecond/1e6))

connection = happybase.Connection('bigdata1')
table = connection.table('Datatest')
rows = table.scan(row_start='{0}-{1}'.format(bus_line, start_ts),
                  row_stop='{0}-{1}'.format(bus_line, end_ts))

buses = {}
for key, data in rows:
    veh = data['info:vehicle']
    sp = float(data['info:speed'])
    if veh in buses:
        if buses[veh] < sp:
            buses[veh] = sp
        else:
            buses[veh] = sp

for bus in buses.keys():
    print(bus, buses[bus])
