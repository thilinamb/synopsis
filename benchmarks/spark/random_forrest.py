from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
import json
import time
import math
import random as rnd
import pyspark
from pyspark import SparkConf, SparkContext
import numpy as np

def parse_json_record(rec):
	ind_var = ('visibility_surface', 'relative_humidity_zerodegc_isotherm', 'precipitable_water_entire_atmosphere')
	if(len(rec) > 0):
		vals = [float(rec[x]) for x in ind_var]
		return LabeledPoint(rec['temperature_surface'], vals)

def load_json(x):
	try:
		return json.loads(x)
	except json.JSONDecodeError:
		return []

def main():
	conf = (SparkConf()
         .setMaster('spark://lattice-10.cs.colostate.edu:11011')
         .setAppName('Random Forrest Benchmark')
         .set("spark.executor.memory", "2g").set('spark.executor.cores', '2'))
	sc = SparkContext(conf = conf)
	sc.setLogLevel('ERROR')

        # HDFS Namenode
	hdfs_nn = 'hdfs://lattice-100.cs.colostate.edu:46780/'
        
    # load time test
	input_list = ('test/southeast-0*-*.txt', 'sample/sample-10.txt', 'sample/sample-20.txt', 'syn/southeast-synth-1.txt', '/syn/southeast-synth-2.txt')
	load_times = []        
	for i in range(6):
		load_time_i = []
		for input_path in input_list:
			t1 = time.time()
			data = sc.textFile(hdfs_nn + input_path)
			data.first()
			t2 = time.time()
			load_time_i.append(t2 - t1)
		load_times.append(load_time_i)
		print('Load time iteration completed. iteation: ', i)

	load_time_data = np.array(load_times)
	print('Load time data shape: ', load_time_data.shape)
	np.save('load_times.npy', load_time_data)

        # load input data
	noaa_raw = sc.textFile(hdfs_nn + 'test/southeast-0*-*.txt')
	noaa_json=noaa_raw.map(load_json).filter(lambda x: len(x) > 0)
	noaa_labeled = noaa_json.map(parse_json_record)
	print('Full Dataset Parition Count: ', noaa_labeled.getNumPartitions())

	syn = sc.textFile(hdfs_nn + 'syn/southeast-synth-1.txt')
	syn_json=syn.map(load_json).filter(lambda x: len(x) > 0)
	syn_labeled = syn_json.map(parse_json_record)    
	print('Synthetic 10% Parition Count: ', syn_labeled.getNumPartitions())

	syn2 = sc.textFile(hdfs_nn + '/syn/southeast-synth-2.txt')
	syn2_json=syn2.map(load_json).filter(lambda x: len(x) > 0)
	syn2_labeled = syn2_json.map(parse_json_record)
	print('Synthetic 20% Parition Count: ', syn2_labeled.getNumPartitions())

	#noaa_labeled.cache()
	#syn_labeled.cache()
	#syn2_labeled.cache()

	noaa_10_raw = sc.textFile(hdfs_nn + 'sample/sample-10.txt')
	noaa_10_json = noaa_10_raw.map(load_json).filter(lambda x: len(x) > 0)
	noaa_10 = noaa_10_json.map(parse_json_record)
	print('10% Sample Parition Count: ', noaa_10.getNumPartitions())

	noaa_20_raw = sc.textFile(hdfs_nn + 'sample/sample-20.txt')
	noaa_20_json = noaa_20_raw.map(load_json).filter(lambda x: len(x) > 0)
	noaa_20 = noaa_20_json.map(parse_json_record)
	print('20% Sample Parition Count: ', noaa_20.getNumPartitions())

	#noaa_10.cache()
	#noaa_20.cache()

	print('Record Counts - Raw: ', noaa_labeled.count())
	print('Record Counts - Syn 10%: ', syn_labeled.count())
	print('Record Counts - Syn 20%: ', syn2_labeled.count())
	print('Record Counts - Raw 10%: ', noaa_10.count())
	print('Record Counts - Raw 20%: ', noaa_20.count())

	iteration_count = 7
	out_f = '/s/chopin/a/grad/thilinab/research/synopsis_pyspark_sql/spark-random-forr-' + str(time.time()) + '.csv'
	print('Output file: ' + out_f)
	for i in range(iteration_count):
		print('Starting the iteration: ', i)
		it_start = time.time()
		# training and test RDDs
		training_rdd, test_rdd = noaa_labeled.randomSplit(weights=[0.95, 0.05], seed=rnd.randint(100,2000))
		# cache them
		#training_rdd.cache()
		#test_rdd.cache()

    	# training phase
    	# raw
		t_before_train = time.time()
		model = RandomForest.trainRegressor(training_rdd,
                                        categoricalFeaturesInfo={}, maxDepth=20, 
                                numTrees=32, maxBins=32)
		t_after_train = time.time()

    	# raw - 10%
		noaa_10_start = time.time()
		noaa_10_model = RandomForest.trainRegressor(noaa_10,
                                        categoricalFeaturesInfo={}, maxDepth=20, 
                                        numTrees=32, maxBins=32)
		noaa_10_stop = time.time()

    	# raw - 20%
		noaa_20_start = time.time()
		noaa_20_model = RandomForest.trainRegressor(noaa_20,
                                        categoricalFeaturesInfo={}, maxDepth=20, 
                                        numTrees=32, maxBins=32)
		noaa_20_stop = time.time()

    	# syn - 10%
		syn_t_start = time.time()
		syn_model = RandomForest.trainRegressor(syn_labeled,
                                        categoricalFeaturesInfo={}, maxDepth=20, 
                                        numTrees=32, maxBins=32)
		syn_t_stop = time.time()

    	# syn - 20%
		syn2_t_start = time.time()
		syn2_model = RandomForest.trainRegressor(syn2_labeled,
                                    categoricalFeaturesInfo={}, maxDepth=20, 
                                    numTrees=32, maxBins=32)
		syn2_t_stop = time.time()



    	# prediction phase
    	# raw
		predictions = model.predict(test_rdd.map(lambda x: x.features))
		labelsAndPredictions = test_rdd.map(lambda lp: lp.label).zip(predictions)
		testMSE = labelsAndPredictions.map(lambda v: (v[0] - v[1]) * (v[0] - v[1])).sum()/float(test_rdd.count())

    	# raw - 10%
		noaa_10_predictions = noaa_10_model.predict(test_rdd.map(lambda x: x.features))
		noaa_10_labelsAndPredictions = test_rdd.map(lambda lp: lp.label).zip(noaa_10_predictions)
		noaa_10_testMSE = noaa_10_labelsAndPredictions.map(lambda v: (v[0] - v[1]) * (v[0] - v[1])).sum() / float(test_rdd.count())

    	# raw - 20%
		noaa_20_predictions = noaa_20_model.predict(test_rdd.map(lambda x: x.features))
		noaa_20_labelsAndPredictions = test_rdd.map(lambda lp: lp.label).zip(noaa_20_predictions)
		noaa_20_testMSE = noaa_20_labelsAndPredictions.map(lambda v: (v[0] - v[1]) * (v[0] - v[1])).sum() / float(test_rdd.count())

    	# syn - 10%
		syn_predictions = syn_model.predict(test_rdd.map(lambda x: x.features))
		syn_labelsAndPredictions = test_rdd.map(lambda lp: lp.label).zip(syn_predictions)
		syn_testMSE = syn_labelsAndPredictions.map(lambda v: (v[0] - v[1]) * (v[0] - v[1])).sum() / float(test_rdd.count())

    	# syn - 20%
		syn2_predictions = syn2_model.predict(test_rdd.map(lambda x: x.features))
		syn2_labelsAndPredictions = test_rdd.map(lambda lp: lp.label).zip(syn2_predictions)
		syn2_testMSE = syn2_labelsAndPredictions.map(lambda v: (v[0] - v[1]) * (v[0] - v[1])).sum() / float(test_rdd.count())

		out_str = '{},{},{},{},{},{},{},{},{},{}\n'.format(
    			(t_after_train - t_before_train),
    			(noaa_10_stop - noaa_10_start),
    			(noaa_20_stop - noaa_20_start),
    			(syn_t_stop - syn_t_start),
    			(syn2_t_stop - syn2_t_start),
    			math.sqrt(testMSE),
    			math.sqrt(noaa_10_testMSE),
    			math.sqrt(noaa_20_testMSE),
    			math.sqrt(syn_testMSE),
    			math.sqrt(syn2_testMSE))
    	
		print(out_str)
    	# write to file
		with open(out_f, 'a') as file_handler:
			file_handler.write(out_str)

    	# uncache training and test RDDs
		#training_rdd.unpersist()
		#test_rdd.unpersist()
		it_end = time.time()
		print('Iteration Completion Time: ', (it_end - it_start))
		time.sleep(120)

	print('Completed all iterations.')

if __name__ == '__main__':
	main()
