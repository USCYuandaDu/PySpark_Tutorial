from pyspark import SparkContext
sc = SparkContext("local", "test")
lines = sc.textFile("data.txt")
counts = lines.map(lambda l : (l, 1)).reduceByKey(lambda a,b : a + b)
print counts.collect()

