from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from fuzzywuzzy import fuzz
import Levenshtein
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import re

spark = SparkSession.builder \
    .appName("Your App Name") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

joinDf = spark.read.orc("/home/labuser/Desktop/Persistant_Folder/Joined_DF/")



def letter_to_digit(phone):
    # Mapping letters to digits
    keypad_mapping = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "22233344455566677778889999")
    return phone.upper().translate(keypad_mapping)


def normalize_phone_number(phone):
    # Convert letters to digits
    phone = letter_to_digit(phone)
    # Remove non-digit characters except for the leading '+'
    phone = re.sub(r'[^+\d]', '', phone)
    
    # Ensure the phone starts with '+', if not, add a default country code (e.g., +1 for US)
    if not phone.startswith('+'):
        phone = '+1' + phone  # Adjust the default country code as needed
    
    return phone[1:]  # Remove the '+' for further processing


# Function to calculate similarity scores after normalizing phone numbers
def calculate_similarity_scores(phone1, phone2):
    # Normalize the phone numbers
    normalized_phone1 = normalize_phone_number(phone1)
    normalized_phone2 = normalize_phone_number(phone2)

    # Calculate similarity scores
    token_set_ratio_phone = fuzz.token_set_ratio(normalized_phone1, normalized_phone2)
    partial_ratio_phone = fuzz.partial_ratio(normalized_phone1, normalized_phone2)
    
    # Calculate Levenshtein distance and similarity
    lev_distance_phone = Levenshtein.distance(normalized_phone1, normalized_phone2)
    max_len_phone = max(len(normalized_phone1), len(normalized_phone2))
    lev_similarity_phone = (1 - lev_distance_phone / max_len_phone) * 100 if max_len_phone > 0 else 0
    
    # Confidence score calculation
    confidence_score_phone = (0.4 * token_set_ratio_phone) + (0.4 * partial_ratio_phone) + (0.2 * lev_similarity_phone)

    return confidence_score_phone
# Convert both phone numbers into vectors, using a fixed max length

def compare(row):
    name1 = row["name1"]
    name2 = row["name2"]
    email1 = row["email1"].split('@')[0]  # Get part before '@'
    email2 = row["email2"].split('@')[0] 
    
    explanation = []

    # Compare names using fuzzy matching
    token_set_ratio_name = fuzz.token_set_ratio(name1, name2)
    partial_ratio_name = fuzz.partial_ratio(name1, name2)
    lev_distance_name = Levenshtein.distance(name1, name2)
    max_len_name = max(len(name1), len(name2))
    lev_similarity_name = (1 - lev_distance_name / max_len_name) * 100 if max_len_name > 0 else 0
    confidence_score_name = (0.4 * token_set_ratio_name) + (0.4 * partial_ratio_name) + (0.2 * lev_similarity_name)
    
    if confidence_score_name > 60:
        explanation.append("Name Matching")
    else:
        explanation.append("Name Do Not Match")

    # Compare emails using fuzzy matching
    token_set_ratio_email = fuzz.token_set_ratio(email1, email2)
    partial_ratio_email = fuzz.partial_ratio(email1, email2)
    lev_distance_email = Levenshtein.distance(email1, email2)
    max_len_email = max(len(email1), len(email2))
    lev_similarity_email = (1 - lev_distance_email / max_len_email) * 100 if max_len_email > 0 else 0
    confidence_score_email = (0.4 * token_set_ratio_email) + (0.4 * partial_ratio_email) + (0.2 * lev_similarity_email)
    
    if confidence_score_email > 60:
        explanation.append("Email Matching")
    else:
        explanation.append("Email Do Not Match")

    # Compare phones
    phone1 = row["phone1"]
    phone2 = row["phone2"]
    if(phone1=="" or phone2==""):
          explanation.append("Phone do not Match")
          return (row["tid1"], row["tid2"], ", ".join(explanation))
    similarity = calculate_similarity_scores(phone1, phone2)
    if similarity>80:
        explanation.append("Phone Matching")
    else:
        explanation.append("Phone Do Not Match")

    # Return the combined explanations
    return (row["tid1"], row["tid2"], ", ".join(explanation))

# Process the DataFrame using map
result_rdd = joinDf.rdd.map(compare)

# Define schema for the result DataFrame
schema = StructType([
    StructField("tid1", StringType(), True),
    StructField("tid2", StringType(), True),
    StructField("explanation", StringType(), True)
])

# Convert RDD back to DataFrame
resultDf = spark.createDataFrame(result_rdd, schema)

# Read star graph DataFrame
star = spark.read.orc("/home/labuser/Desktop/star_graph/")
finalDf = resultDf.join(star, resultDf["tid1"] == star["source_system_id"], "left")

# Show final results
finalDf=finalDf.select("cluster_id", "tid1", "tid2", "explanation")

# Assuming 'finalDf' is your DataFrame

# Write the DataFrame partitioned by 'cluster_id' and save it as an ORC file
output_path = "/home/labuser/Desktop/Persistant_Folder/clusterDf/"

finalDf.write.partitionBy("cluster_id").orc(output_path)


