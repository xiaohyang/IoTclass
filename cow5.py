
"""
 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iot-sql.py \
      localhost:9092 test`
"""
from __future__ import print_function

import sys
import re
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import OffsetRange
from operator import add

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    sqlContext = SQLContext(sc)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    jsonDStream = kvs.map(lambda (key, value): value)

    # Define function to process RDDs of the json DStream to convert them
    #   to DataFrame and run SQL queries
    def process(time, rdd):
        print("========= %s =========" % str(time))

        try:
            # Parse the one line JSON string from the DStream RDD
	    jsonString = rdd.map(lambda x: \
		re.sub(r"\s+", "", x, flags=re.UNICODE)).reduce(add)
	    print("jsonString = %s" % str(jsonString))

            # Convert the JSON string to an RDD
            jsonRDDString = sc.parallelize([str(jsonString)])

            # Convert the JSON RDD to Spark SQL Context
	    jsonRDD = sqlContext.read.json(jsonRDDString)

            # Register the JSON SQL Context as a temporary SQL Table
	    print("JSON Schema\n=====")
            jsonRDD.printSchema()
            jsonRDD.registerTempTable("iotmsgsTable")

            #############
            # Processing and Analytics go here
            #############

            # Sort
            sqlContext.sql("select payload.data.ID, payload.data.temperature from iotmsgsTable order by temperature desc").show(n=100)
           
            # Filter
            sqlContext.sql("select payload.data.* from iotmsgsTable where payload.data.temperature > 39.6").show(n=100)

            # Sort
            sqlContext.sql("select payload.data.ID, payload.data.rumination from iotmsgsTable order by rumination asc").show(n=100)
           
            # Filter
            sqlContext.sql("select payload.data.* from iotmsgsTable where payload.data.rumination > 400.0").show(n=100)
            
            # Alert
            std = sqlContext.sql("select STDEVP(payload.data.rumination) from iotmsgsTable") 
            print ("standard deviation =" std)
            if std > 80.0:
                print ("Please check the herd")
            else:
                print ("Herd is ok")
            

            # Clean-up
	    sqlContext.dropTempTable("iotmsgsTable")
        # Catch any exceptions
        except:
            pass

    # Process each RDD of the DStream coming in from Kafka
    jsonDStream.foreachRDD(process)
 
    ssc.start()
    ssc.awaitTermination()
