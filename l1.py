# from pyspark import SparkContext
# import pyspark
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
import json

sc = SparkContext(appName="streaming")
spark = SparkSession(sc)
spark.sparkContext.setLogLevel('ERROR')

    #spark streaming context
ssc = StreamingContext(sc,batchDuration=20)

    #dstream from streaming server
dstream = ssc.socketTextStream("localhost",8083)

sum1=0

l1=dstream.count()

l1.pprint()


ssc.start()
ssc.awaitTermination()




