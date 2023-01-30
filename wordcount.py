import sys
import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
conf = SparkConf().setAppName("wordCount")
sc = SparkContext(conf=conf)

text_file = sc.textFile("/user/anair22/pySparkWordCount/oozie/input.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("/user/anair22/pySparkWordCount/oozie/outputPySpark/")
