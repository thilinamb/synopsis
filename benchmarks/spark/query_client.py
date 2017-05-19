from pyspark.sql import SparkSession
import random
import time
import threading
import numpy as np

def rand_query(queries):
    q = queries[random.randint(0, len(queries) - 1)]
    return q

def query(latencies, queries):
    time.sleep(5)
    for j in range(10):
        t1 = time.time()
        spark.sql(rand_query(queries)).count()
        t2 = time.time()
        latencies.append(t2 - t1)


if __name__ == '__main__':
    input_path = 'hdfs://lattice-100.cs.colostate.edu:46780/test/southeast-0*-*.txt'
    
    spark = SparkSession.builder.appName("Python Spark SQL basic example").master('spark://lattice-10.cs.colostate.edu:11011').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # read data
    df = spark.read.json(input_path)
    df.printSchema()

    # set the seed for random query selection 
    random.seed(123)
    # load queries from the file
    queries = [line.rstrip('\n') for line in open('sql-queries.txt')]

    # temporary view for Spark SQL
    df.createOrReplaceTempView('noaa_data')
    
    threads = []
    latencies = []
    thread_count = (1,2)
    for t_c in thread_count:
        print('\nStarting to run with ', t_c, ' threads.')
        for i in range(t_c):
            t = threading.Thread(target=query, args=(latencies, queries))
            threads.append(t)
            t.start()
            # wait until last one finishes
            if i == t_c - 1:
                t.join()
        
        # wait until everyone finishes
        time.sleep(60)
        lat_data = np.array(latencies)
        with open('/tmp/spark-sql-' + str(t_c) + '.csv', 'w') as file_handler:
            for item in latencies:
                file_handler.write("{}\n".format(item))
        print('Thread-Count: ', t_c,' Mean: ', np.mean(lat_data), ', Std. Dev: ', np.std(lat_data), '95th Percentile: ',np.percentile(lat_data, 95))
        time.sleep(120)

