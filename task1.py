from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from fuzzywuzzy import fuzz
import Levenshtein
import re

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Your App Name") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Load joinDf and star DataFrames
joinDf = spark.read.orc("/home/labuser/Desktop/Persistant_Folder/Joined_DF/")
star = spark.read.orc("/home/labuser/Desktop/star_graph/")

# Function to normalize phone numbers
def letter_to_digit(phone):
    keypad_mapping = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "22233344455566677778889999")
    return phone.upper().translate(keypad_mapping)

def normalize_phone_number(phone):
    phone = letter_to_digit(phone)
    phone = re.sub(r'[^+\d]', '', phone)
    if not phone.startswith('+'):
        phone = '+1' + phone  # Adjust the default country code as needed
    return phone[1:]

# Function to calculate phone similarity scores
def calculate_similarity_scores(phone1, phone2):
    normalized_phone1 = normalize_phone_number(phone1)
    normalized_phone2 = normalize_phone_number(phone2)
    token_set_ratio_phone = fuzz.token_set_ratio(normalized_phone1, normalized_phone2)
    partial_ratio_phone = fuzz.partial_ratio(normalized_phone1, normalized_phone2)
    lev_distance_phone = Levenshtein.distance(normalized_phone1, normalized_phone2)
    max_len_phone = max(len(normalized_phone1), len(normalized_phone2))
    lev_similarity_phone = (1 - lev_distance_phone / max_len_phone) * 100 if max_len_phone > 0 else 0
    confidence_score_phone = (0.4 * token_set_ratio_phone) + (0.4 * partial_ratio_phone) + (0.2 * lev_similarity_phone)
    return confidence_score_phone

# Comparison function for rows
def compare(row):
    name1 = row["name1"]
    name2 = row["name2"]
    email1 = row["email1"].split('@')[0]
    email2 = row["email2"].split('@')[0]
    p2pe1 = row["p2pe1"]
    p2pe2 = row["p2pe2"]
    address1 = row["address1"]
    address2 = row["address2"]
    
    explanation = []

    # Name comparison
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
    
    explanation.append("\n")  # New line after Name comparison

    # Email comparison
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
    
    explanation.append("\n")  # New line after Email comparison

    # P2PE comparison
    if not p2pe1 or not p2pe2:
        explanation.append("P2PE Not Matching")
    else:
        if p2pe1 == p2pe2:
            explanation.append("P2PE Matching")
        else:
            explanation.append("P2PE Not Matching")
    
    explanation.append("\n")  # New line after P2PE comparison

    # Phone comparison
    phone1 = row["phone1"]
    phone2 = row["phone2"]
    if not phone1 or not phone2:
        explanation.append("Phone Do Not Match")
    else:
        similarity = calculate_similarity_scores(phone1, phone2)
        if similarity > 80:
            explanation.append("Phone Matching")
        else:
            explanation.append("Phone Do Not Match")
    
    explanation.append("\n")  # New line after Phone comparison

    # Address comparison
    if address1 and address2:  # Only compare if both addresses are provided
        token_set_ratio_address = fuzz.token_set_ratio(address1, address2)
        partial_ratio_address = fuzz.partial_ratio(address1, address2)
        lev_distance_address = Levenshtein.distance(address1, address2)
        max_len_address = max(len(address1), len(address2))
        lev_similarity_address = (1 - lev_distance_address / max_len_address) * 100 if max_len_address > 0 else 0
        confidence_score_address = (0.4 * token_set_ratio_address) + (0.4 * partial_ratio_address) + (0.2 * lev_similarity_address)
        
        if confidence_score_address > 60:
            explanation.append("Address Matching")
        else:
            explanation.append("Address Do Not Match")
    else:
        explanation.append("Address Not Provided")
    
    # Return combined explanations with section breaks
    return (row["tid1"],name1,email1,phone1,address1,p2pe1,row["tid2"],name2,email2,phone2,address2,p2pe2, "".join(explanation))

# Process the DataFrame using map
result_rdd = joinDf.rdd.map(compare)

# Define schema for the result DataFrame
schema = StructType([
    StructField("tid1", StringType(), True),
    StructField("name1", StringType(), True),
    StructField("email1", StringType(), True),
    StructField("phone1", StringType(), True),
    StructField("address1", StringType(), True),
    StructField("p2pe1", StringType(), True),
    StructField("tid2", StringType(), True),
    StructField("name2", StringType(), True),
    StructField("email2", StringType(), True),
    StructField("phone2", StringType(), True),
    StructField("address2", StringType(), True),
    StructField("p2pe2", StringType(), True),
    StructField("explanation", StringType(), True)
])

# Convert RDD back to DataFrame
resultDf = spark.createDataFrame(result_rdd, schema)

# Join resultDf with star DataFrame to include 'cluster_id' and 'source_system_id'
finalDf = resultDf.join(star, resultDf["tid1"] == star["source_system_id"], "left")

# Select columns in specified order
finalDf = finalDf.select(
    "cluster_id","tid1", "name1", "phone1", "email1", "address1", "p2pe1", 
    "tid2", "name2", "phone2", "email2", "address2", "p2pe2", "explanation"
)

# Show the final schema
finalDf.printSchema()

# Save to ORC file if desired
output_path = "/home/labuser/Desktop/Persistant_Folder/finalDf2/"
# Uncomment the line below to write the final DataFrame to ORC format
finalDf.write.orc(output_path)

