from pymongo import MongoClient

db = MongoClient().BDProcessing
bus_data = db.BusData

pipeline = [
    {"$unwind": "$body"},
    {"$group": {"_id": "$body.monitoredVehicleJourney.lineRef",
                "vehRef": {"$addToSet": "$body.monitoredVehicleJourney.vehicleRef"}}},
    {"$project": {"_id": 0, "lineRef": "$_id", "vehicleCount": {"$size": "$vehRef"}}}
]

veh_counted = list(bus_data.aggregate(pipeline))

print('lineRef, vehicleCount')
for line in veh_counted:
    line = dict([(str(k), str(v)) for k, v in line.items()])
    print('{0}, {1}'.format(line['lineRef'], line['vehicleCount']))

