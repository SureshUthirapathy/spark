import pyspark
import pytest
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


test_files = ["final_data.csv","distinct_makers.csv","top_10_city_mpg.csv","top_10_mpg.csv","top_10_price.csv",
		"safety.csv","corr.csv","four_cylinder.csv","car_class.csv","stat.csv","safe_maker.csv"]

format_dict = {
		"final_data.csv":['symboling','normalized-losses','make','fuel-type','aspiration','num-of-doors','body-style',
		'drive-wheels','engine-location','wheel-base','length','width','height','curb-weight','engine-type','num-of-cylinders',
		'engine-size','fuel-system','bore','stroke','compression-ratio','horsepower','peak-rpm','city-mpg','highway-mpg','price'],		
		"distinct_makers.csv":["make"],
		"top_10_city_mpg.csv":["make","city-mpg"],
		"top_10_mpg.csv":["make","mpg"],
		"top_10_price.csv":["make","price"],
		"safety.csv":["Safety","count"],
		"corr.csv":["correlation"],
		"four_cylinder.csv":["make"],
		"car_class.csv":["title","count"],
		"stat.csv":["avgMpg","minMpg","maxMpg"],
        "safe_maker.csv":["make","total"]}


exist = []
not_exist = []
bad_format = []


def test_file_exist_hdfs():
	#check if file is available in hdfs. if yes, call check_file_format to validate the header information. 
	for i in range(len(test_files)):
		try:
			rdd = spark.sparkContext.textFile(test_files[i])
			rdd.take(1)
			exist.append(test_files[i])
		except Py4JJavaError as e:
			not_exist.append(test_files[i])

	assert not not_exist, "Oops! looks like one or more files required for scoring are missing in HDFS. If you think they have been created, please recheck whether the file name matches what is given in the instruction including .csv. Note that you will still be able to submit for final scoring, but only the available files will pass through scoring. The files we think missing are: {}".format(not_exist)
	
@pytest.fixture
def exist_files():
    return exist

def test_check_file_format(exist_files):
	for file in exist_files:
		expected_cols = format_dict.get(file)
		if expected_cols == None:
			print("File format not defined")
		else:
			data = spark.read.csv(file, header=True, inferSchema=True)
			actual_cols = []
			[actual_cols.append(data.dtypes[i][0]) for i in range(len(data.dtypes))]
			if expected_cols != actual_cols:
				bad_format.append([file,expected_cols,actual_cols])
			

	assert not bad_format, "Looks like few output csv files do not have expected header column names. They need to match the expected column names as given in the instruction. Given below are the files we think are having header format issues. Read in order - [file name, expected cols, actual cols] {}".format(bad_format)
