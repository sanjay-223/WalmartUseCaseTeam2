val path="/home/labuser/Desktop/cust_profiles/"
val df=spark.read.format("orc").load(path)
df.show()
val summaryDF = df.describe()
