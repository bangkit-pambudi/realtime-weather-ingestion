from pyspark import SparkContext                                                                                        
from pyspark.sql import SparkSession                                                                                    
from pyspark.streaming import StreamingContext                                                                          
from pyspark.streaming.kafka import KafkaUtils
                                                                          
                                                                                                                        
def handle_rdd(rdd):                                                                                                    
    if not rdd.isEmpty():                                                                                               
        global ss                                                                                                       
        df = ss.createDataFrame(rdd, schema=['uuid', 'city', 'localtime_epoch', 'localtime', 'last_updated_epoch', 'last_updated', 'temp_c', 'temp_f', 'wind_mph', 'wind_kph', 'wind_degree'])                                                
        df.show()                                                                                             
        df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="weather", keyspace="codex").save()                                       
                                                                                                                        
sc = SparkContext(appName="Something")                                                                                     
ssc = StreamingContext(sc, 5)                                                                                           
                                                                                                                        
ss = SparkSession.builder.appName("CassandraIntegration").config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()
                                                                                                                        
ss.sparkContext.setLogLevel('WARN')                                                                                     
                                                                                                                        
ks = KafkaUtils.createDirectStream(ssc, ['weather'], {'metadata.broker.list': 'localhost:9092'})                       
                                                                                                                        
lines = ks.map(lambda x: x[1])                                                                                                                                                                                                                

transform = lines.map(lambda x: x.split(','))

transform.foreachRDD(handle_rdd)                                                                                       
                                                                                                                        
ssc.start()                                                                                                             
ssc.awaitTermination()