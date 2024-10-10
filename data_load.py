from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("load datas").getOrCreate()
cust_profile=spark.read.orc("/home/labuser/Desktop/CaseStudy2/cust_profiles")
raw_graph=spark.read.orc("/home/labuser/Desktop/CaseStudy2/raw_graph")
star_graph=spark.read.orc("/home/labuser/Desktop/CaseStudy2/star_graph")
cust_profile.show(20)
raw_graph.show(20)
star_graph.show(20)

cust_profile.printSchema()
raw_graph.printSchema()
star_graph.printSchema()