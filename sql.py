from pyspark.sql import SparkSession

# Step 1: Set up Spark session with increased memory allocation and overhead
spark = SparkSession.builder \
    .appName("ORCtoMySQL") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.memoryOverhead", "2g") \
    .getOrCreate()

# Path to the ORC file (update with the actual path)
orc_file_path = "/home/labuser/Desktop/Persistant_Folder/finalDf/"  # Update this path
df = spark.read.orc(orc_file_path)

# Show the schema to understand the structure (optional)
df.printSchema()

# Step 2: Define MySQL connection parameters
mysql_url = "jdbc:mysql://localhost:3306/usecase2"  # Replace with your database URL
mysql_properties = {
    "user": "root",
    "password": "Root123$",  # Replace with your actual password
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Define the table name in MySQL
table_name = "explanation1"

# Step 3: Write the DataFrame directly to MySQL
df.select(
    "cluster_id", "tid1", "name1", "email1", "phone1", "address1", "p2pe1",
    "tid2", "name2", "email2", "phone2", "address2", "p2pe2", "explanation"
).write \
    .jdbc(url=mysql_url, table=table_name, mode="append", properties=mysql_properties)

print(f"Data successfully written to MySQL table '{table_name}'.")

# Optional: Create an index on `cluster_id` column
# Connect to MySQL and execute an index creation query separately
import mysql.connector

# Step 4: Define MySQL connection parameters
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Root123$",  # Replace with your actual password
    database="usecase2"
)

cursor = connection.cursor()
index_query = f"CREATE INDEX IF NOT EXISTS idx_cluster_id ON {table_name} (cluster_id)"
cursor.execute(index_query)
print("Index created on 'cluster_id' column.")

# Close the cursor and connection
cursor.close()
connection.close()
print("MySQL connection closed.")

# Stop Spark session
spark.stop()

