from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, Rating

if __name__ == '__main__':
    APP_NAME = "PySpark-CF"
    conf = SparkConf().setAppName(APP_NAME).setMaster("yarn-client")\
        .set("spark.driver.cores", 2)
    sc = SparkContext(conf=conf)
    evt_data = sc.textFile("events.csv")
    prop_data1 = sc.textFile("item_properties_part1.csv")
    prop_data2 = sc.textFile("item_properties_part2.csv")

    evt_head = evt_data.take(1)[0]
    events  = evt_data.filter(lambda l: l != evt_head)\
        .map(lambda l: l.split(',')).map(lambda l: (l[3], (l[1], l[2], l[4])))
    
    prop_head = prop_data1.take(1)[0]
    prop_data  = prop_data1.filter(lambda l: l != prop_head)\
        .union(prop_data2.filter(lambda l: l != prop_head))\
        .map(lambda l: l.split(',')).map(lambda l: (l[1], (l[2], l[3])))

    joined = events.join(prop_data)\
        .map(lambda l: (l[0], l[1][0][0], l[1][0][1], l[1][0][2], l[1][1][0], l[1][1][1]))\
        .filter(lambda l: l[4].isdigit() and float(l[4]) > 100)

    train = joined.filter(lambda l: int(l[1]) != 257597)\
        .map(lambda l: Rating(int(l[1]), int(l[0]), float(l[4])))

    test = joined.filter(lambda l: int(l[1]) == 257597)\
        .map(lambda l: Rating(int(l[1]), int(l[0])))

    rank = 10
    numIterations = 10

    model = ALS.train(train, rank, numIterations)

    # Evaluate the model on training data
    predictions = model.predictAll(test).map(lambda r: ((r[0], r[1]), r[2]))
    predictions.saveAsTextFile('recc')

