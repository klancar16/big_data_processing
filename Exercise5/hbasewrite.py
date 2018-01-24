import happybase
import json
import pydoop.hdfs as hdfs
import time as mod_time
from datetime import datetime

with hdfs.open('bd_klancar_j.json') as f:
    bus_data = json.load(f)

connection = happybase.Connection('bigdata1')
table = connection.table('Datatest')

for bus_el in bus_data:
    for ride in bus_el['body']:
        ride_time = datetime.strptime(ride['recordedAtTime'].split('+')[0], "%Y-%m-%dT%H:%M:%S.%f")
        ride_ts = int((mod_time.mktime(ride_time.timetuple())+ride_time.microsecond/1e6))
        r_key = '{0}-{1}'.format(ride['monitoredVehicleJourney']['lineRef'], ride_ts)
        lat = ride['monitoredVehicleJourney']['vehicleLocation']['latitude']
        long = ride['monitoredVehicleJourney']['vehicleLocation']['longitude']
        veh = ride['monitoredVehicleJourney']['vehicleRef']
        speed = ride['monitoredVehicleJourney']['speed']

        table.put(r_key, {b'info:vehicle': veh,
                          b'info:lat': lat,
                          b'info:lon': long,
                          b'info:speed': speed})
