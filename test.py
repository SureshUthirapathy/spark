import findspark
findspark.init()
import pyspark
import pytest
import pyspark.sql
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import types as T
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from py4j.protocol import Py4JJavaError
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import StringType,IntegerType,DateType

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

df = spark.read.csv("Automobile_data.csv",header=True)

df = df.withColumn('symboling' , df["symboling"].cast('int'))
df = df.withColumn('normalized-losses' , df["normalized-losses"].cast('int'))
df = df.withColumn('bore' , df["bore"].cast('double'))
df = df.withColumn('stroke' , df["stroke"].cast('double'))
df = df.withColumn('horsepower' , df["horsepower"].cast('double'))
df = df.withColumn('peak-rpm' , df["peak-rpm"].cast('double'))
df = df.withColumn('price' , df["price"].cast('double'))
df = df.withColumn('curb-weight' , df["curb-weight"].cast('int'))
df = df.withColumn('length' , df["length"].cast('double'))
df = df.withColumn('wheel-base' , df["wheel-base"].cast('double'))
df = df.withColumn('width' , df["width"].cast('double'))
df = df.withColumn('height' , df["height"].cast('double'))
df = df.withColumn('engine-size' , df["engine-size"].cast('int'))
df = df.withColumn('compression-ratio' , df["compression-ratio"].cast('double'))
df = df.withColumn('city-mpg' , df["city-mpg"].cast('int'))
df = df.withColumn('highway-mpg' , df["highway-mpg"].cast('int'))

df = df.fillna(122.0 , subset = ['normalized-losses'])

bore_mean = df.select(avg("bore")).collect()[0]['avg(bore)']
df = df.fillna(bore_mean , subset = ['bore'])

stroke_mean = df.select(avg("stroke")).collect()[0]['avg(stroke)']
df = df.fillna(stroke_mean , subset = ['stroke'])

avg_hp = df.select(avg("horsepower")).collect()[0]['avg(horsepower)']
df = df.fillna(avg_hp , subset = ['horsepower'])

avg_rpm = df.select(avg("peak-rpm")).collect()[0]['avg(peak-rpm)']
df = df.fillna(avg_rpm , subset = ['peak-rpm'])

max_door = df.groupby("num-of-doors").count().orderBy("count", ascending=False).first()[0]
df = df.fillna(max_door , subset = ['num-of-doors'])

df = df.where((df.price != 0 ))

e_final_data_df = df.filter(col('normalized-losses').isNotNull())

#Read the necessary actual files from HDFS/local. These are the files that the candidates would have saved during execution.

#Support function to check whether file exists or not.

def check_file_available(file): 
    print("file: ",file)	
    try:
        print('%s does exist' % file)
        rdd = spark.sparkContext.textFile(file)
        rdd.take(1)
        return True
    except Py4JJavaError as e:
        print('%s does not exist' % file)
        return False


#Test 1 - the count of the final data after NULL records are dropped.

def test_final_df_count():

    print("Test case 1:")	
    file_exist = check_file_available("final_data.csv")

    if file_exist:
        a_final_data_df = spark.read.csv('final_data.csv', header=True, inferSchema=True)
        assert a_final_data_df.count() == e_final_data_df.count()
    else:
        assert file_exist

#Test 2 - the accuracy of datatypes.

def test_final_df_schema():

    print("Test case 2:")		
    file_exist = check_file_available("final_data.csv")

    if file_exist:
        a_final_data_df = spark.read.csv('final_data.csv', header=True, inferSchema=True)
        assert a_final_data_df.dtypes == e_final_data_df.dtypes
    else:
        assert file_exist

#Test 3 - the count of distinct makers

def test_distinct_makers():

    print("Test case 3:")	
    file_exist = check_file_available("distinct_makers.csv")

    if file_exist:
        e_distinct_makers_df = e_final_data_df.select(col('make')).distinct()
        a_distinct_makers_df = spark.read.csv('distinct_makers.csv', header=True, inferSchema=True)
        assert a_distinct_makers_df.count() == e_distinct_makers_df.count()
    else:
        assert file_exist

#Test 4 - What are the top 10 cars in terms of city-mpg

def test_top_10_city_mpg():

    print("Test case 4:")	
    file_exist = check_file_available("top_10_city_mpg.csv")

    if file_exist:
        top_10_city_mpg_df = e_final_data_df[["make","city-mpg"]].sort('city-mpg', ascending=False).head(10)
        rdd1=spark.sparkContext.parallelize(top_10_city_mpg_df)
        e_top_10_city_mpg_df=spark.createDataFrame(rdd1)
        a_top_10_city_mpg_df = spark.read.csv('top_10_city_mpg.csv', header=True, inferSchema=True)
        assert a_top_10_city_mpg_df.subtract(e_top_10_city_mpg_df).count() == 0
    else:
        assert file_exist

#Test 5 - What are the top 10 cars in terms of mpg

def test_top_10_mpg():

    print("Test case 5:")	
    file_exist = check_file_available("top_10_mpg.csv")

    if file_exist:
        top_10_mpg_df = e_final_data_df.withColumn("mpg",(df['city-mpg']+df['highway-mpg'])/2)
        top_10_mpg_df = top_10_mpg_df[['make' ,'mpg']].sort('mpg', ascending=False).head(10)
        rdd2=spark.sparkContext.parallelize(top_10_mpg_df)
        e_top_10_mpg_df=spark.createDataFrame(rdd2)
        a_top_10_mpg_df = spark.read.csv('top_10_mpg.csv', header=True, inferSchema=True)
        assert a_top_10_mpg_df.subtract(e_top_10_mpg_df).count() == 0
    else:
        assert file_exist

#Test 6 - What are the top 10 prices of the available cars ?

def test_top_10_price():

    print("Test case 6:")	
    file_exist = check_file_available("top_10_price.csv")

    if file_exist:
        top_10_price_df = e_final_data_df[['make','price']].sort('price', ascending=False).head(10)
        rdd3= spark.sparkContext.parallelize(top_10_price_df)
        e_top_10_price_df=spark.createDataFrame(rdd3)
        a_top_10_price_df = spark.read.csv('top_10_price.csv', header=True, inferSchema=True)
        assert a_top_10_price_df.subtract(e_top_10_price_df).count() == 0
    else:
        assert file_exist

# Test 7 - Classify cars based on safety scale. Find total based on the classifcation.

def test_safe_classify():

    print("Test case 7:")	
    file_exist = check_file_available("safety.csv")

    if file_exist:
        safe_cat_df = e_final_data_df.withColumn("Safety",f.when(df["symboling"] > 0,'Risky')\
                                .when(df["symboling"] == 0 , 'Neutral')\
                                .when(df["symboling"] <0 , 'Safe')
                                )
        e_safe_classify_df = safe_cat_df.groupBy('Safety')\
                                .agg(count("Safety").alias("count")).orderBy(col("count").asc())
        a_safe_classify_df = spark.read.csv('safety.csv', header=True, inferSchema=True)
        assert a_safe_classify_df.subtract(e_safe_classify_df).count() == 0
    else:
        assert file_exist

# Test 8 - correlation between mpg and curb-weight

def test_corr():

    print("Test case 8:")	
    file_exist = check_file_available("corr.csv")

    if file_exist:
        mpg_df = e_final_data_df.withColumn("mpg",(df['city-mpg']+df['highway-mpg'])/2)
        corr = mpg_df.corr("mpg","curb-weight")
        e_corr_df = spark.createDataFrame([(corr,)],["correlation"])
        e_corr_df = e_corr_df.withColumn("correlation",f.round(f.col('correlation'),2))
        a_corr_df = spark.read.csv('corr.csv', header=True, inferSchema=True)
        a_corr_df1 = a_corr_df.withColumn("correlation",f.round(f.col('correlation'),2))
       
        assert a_corr_df1.subtract(e_corr_df).count() == 0
    else:
        assert file_exist

# Test 9 - Identifying the makers thodse who produce  engine with four cylinder .

def test_four_cylinder():
    print("Test case 9:")
    file_exist = check_file_available("four_cylinder.csv")
    
    if file_exist:
        four_cyl = e_final_data_df.where(e_final_data_df["num-of-cylinders"] == 'four')
        e_four_cyl_maker_df = four_cyl.select("make").distinct()
        a_four_cyl_maker_df = spark.read.csv("four_cylinder.csv" ,header=True ,inferSchema=True)
        assert a_four_cyl_maker_df.subtract(e_four_cyl_maker_df).count() == 0
    else:
        assert file_exist

# Test 10 - Classify cars based on price scale. Find total based on the classifcation.

def test_price_classify():

    print("Test case 10:")	
    file_exist = check_file_available("car_class.csv")

    if file_exist:
        car_class_cat_df = e_final_data_df.withColumn("title",f.when(df["price"] > 25000,'TopClass')\
                                .when(df["price"] >=10000 , 'Mid-Class')\
                                .when(df["price"] <10000 , 'BudgetClass')
                                )
        e_car_classify_df = car_class_cat_df.groupBy('title')\
                                .agg(count("title").alias("count")).orderBy(col("count").asc())
        a_car_classify_df = spark.read.csv('car_class.csv', header=True, inferSchema=True)
        assert a_car_classify_df.subtract(e_car_classify_df).count() == 0
    else:
        assert file_exist
        
# Test 11 - What is the mean, min and max mpg of cars

def test_stat():

    print("Test case 11:")	
    file_exist = check_file_available("stat.csv")

    if file_exist:
        top_10_mpg_df = e_final_data_df.withColumn("mpg",(df['city-mpg']+df['highway-mpg'])/2)
        mpg_df = top_10_mpg_df[['make' ,'mpg']].sort('mpg', ascending=False)
        e_stat_df = mpg_df.select([mean('mpg').cast("int").alias("avgMpg"),min('mpg').alias("minMpg"),max('mpg').alias("maxMpg")])
        a_stat_df = spark.read.csv('stat.csv', header=True, inferSchema=True)
        assert a_stat_df.subtract(e_stat_df).count() == 0
    else:
        assert file_exist
        
# Test 12 - Top Safety car maker

def test_safe_maker():

    print("Test case 12:")	
    file_exist = check_file_available("safe_maker.csv")

    if file_exist:
        safety_cars = e_final_data_df.withColumn("Safety",f.when(df["symboling"] < 0,'safe'))
        e_top_safe_maker = safety_cars.groupBy('make').agg(f.count("Safety").alias("total")).sort('total', ascending=False).limit(1)
        a_top_safe_maker = spark.read.csv('safe_maker.csv' , header=True , inferSchema=True)
        assert a_top_safe_maker.subtract(e_top_safe_maker).count() ==0
    else:
        assert file_exist
