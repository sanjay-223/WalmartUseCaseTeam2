val cust_data = spark.read.format("orc").load("/home/labuser/Desktop/Persistant_Folder/cust_profiles")

cust_data.show()

cust_data.printSchema()

val raw_graph_data = spark.read.format("orc").load("/home/labuser/Desktop/Persistant_Folder/raw_graph")

raw_graph_data.show()

raw_graph_data.printSchema()

val star_graph_data = spark.read.format("orc").load("/home/labuser/Desktop/Persistant_Folder/star_graph")

star_graph_data.show()

star_graph_data.printSchema()

