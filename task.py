from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


spark = SparkSession.builder \
    .appName("Read ORC File") \
    .getOrCreate()

cus=spark.read.orc("/home/labuser/Desktop/cust_profiles/")

raw=spark.read.orc("/home/labuser/Desktop/raw_graph/")
raw=raw.withColumn("Rno", monotonically_increasing_id())


tid1=raw.join(cus,raw["tid1"]==cus["source_system_id"],"left")
tid1=tid1.select(tid1["Rno"],tid1["tid1"],tid1["name"].alias("name1"),tid1["email_id"].alias("email1"),tid1["phone_number"]
.alias("phone1"),tid1["address"].alias("address1"),tid1["p2pe"].alias("p2pe1"))

tid2=raw.join(cus,raw["tid2"]==cus["source_system_id"],"left")
tid2=tid2.select(tid2["Rno"],tid2["tid2"],tid2["name"].alias("name2"),tid2["email_id"].alias("email2"),tid2["phone_number"]
.alias("phone2"),tid2["address"].alias("address2"),tid2["p2pe"].alias("p2pe2"))

join=tid1.join(tid2,tid1["Rno"]==tid2["Rno"],"left")
join=join.withColumn("Sno", monotonically_increasing_id())
join=join.select("tid1","name1","email1","phone1","address1","p2pe1","tid2","name2","email2","phone2","address2","p2pe2")
join.printSchema()
join.write.format("orc").save("/home/labuser/Desktop/Persistant_Folder/Joined_DF")



