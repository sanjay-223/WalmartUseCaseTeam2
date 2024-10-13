val star=spark.read.format("orc").load("/home/labuser/Desktop/star_graph/")
star.createOrReplaceTempView("star")

val cust=spark.read.format("orc").load("/home/labuser/Desktop/cust_profiles/")
cust.createOrReplaceTempView("cust")

val partition=spark.sql("select cluster_id,source_system_id,rank() over(partition by cluster_id order by cluster_id) as rank from star")
partition.createOrReplaceTempView("partition")

val emails=spark.sql("select partition.cluster_id,phone_number,count(*),rank() over(partition by cluster_id order by cluster_id) as rank from partition left join cust on partition.source_system_id=cust.source_system_id group by cluster_id,phone_number")
emails.show() 
