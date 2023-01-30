#! /usr/bin/env python

import sys
from pyspark import SparkConf
from pyspark.sql import HiveContext
from pyspark import SparkContext
from pyspark.sql.functions import col,row_number
from pyspark.sql.types import *

conf = SparkConf().setAppName("ReadFromHive")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)
hivecontext = HiveContext(sc)

nashdaq=hivecontext.sql("select * from dearborndev.nashdaq")
nashdaq= nashdaq.filter(col('date_cob')>= '1972-01-01')
nashdaq= nashdaq.filter(col('date_cob')<= '2018-01-01')
nashdaq.show()

nashdaq.createOrReplaceTempView("nashdaqTempTable") 

sqlContext.sql("create table dearborndev.nashdaq_from_spark_test as select * from nashdaqTempTable");


#nashdaq.write.csv('/u/anair22/PySpark/testDF.csv')
