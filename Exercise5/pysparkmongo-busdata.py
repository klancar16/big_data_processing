from pyspark import SparkContext, SparkConf
import pymongo_spark

conf = SparkConf().setAppName("mongo-busdata")
sc = SparkContext(conf=conf)
pymongo_spark.activate()

# data = spark.read.json('bus.json')
dat = sc.mongoRDD('mongodb://localhost:27017/BDProcessingG16.BusData')\
    .map(lambda x: [(j['monitoredVehicleJourney']['lineRef'], j['monitoredVehicleJourney']['vehicleRef'])
                    for j in x['body']])\
    .flatMap(lambda x: [t for t in x]).distinct().countByKey()

final_str = 'lineRef, vehicleCount\n'
for bus_line in dat.items():
    final_str = final_str + '{0}, {1}\n'.format(str(bus_line[0]), bus_line[1])

with open('question-4-line-vehicles.csv', 'w') as file:
    file.write(final_str)


