from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os
import sys

def main(args):
    # Initialize Spark session with configurations
    spark = SparkSession.builder \
        .appName("Olympics Analysis") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Load datasets, select only necessary columns
    athletes_2012 = spark.read.csv(args[1], header=True, inferSchema=True).select("id", "name", "dob", "height(m)", "weight(kg)", "sport", "country", "num_followers", "num_articles", "personal_best", "event", "coach_id")
    athletes_2016 = spark.read.csv(args[2], header=True, inferSchema=True).select("id", "name", "dob", "height(m)", "weight(kg)", "sport", "country", "num_followers", "num_articles", "personal_best", "event", "coach_id")
    athletes_2020 = spark.read.csv(args[3], header=True, inferSchema=True).select("id", "name", "dob", "height(m)", "weight(kg)", "sport", "country", "num_followers", "num_articles", "personal_best", "event", "coach_id")

    # Add missing columns to athletes_2020 DataFrame
    athletes_2020 = athletes_2020 \
        .withColumn('num_followers', lit(None).cast('int')) \
        .withColumn('num_articles', lit(None).cast('int')) \
        .withColumn('personal_best', lit(None).cast('double')) \
        .withColumn('event', lit(None).cast('string')) \
        .withColumn('coach_id', lit(None).cast('string'))  # Make sure the types match

    # Union the DataFrames
    athletes = athletes_2012.unionByName(athletes_2016).unionByName(athletes_2020)

    # Optionally, cache the DataFrame if it will be reused
    athletes.cache()

    # Write to a more efficient format like Parquet
    output_dir = args[6]
    os.makedirs(output_dir, exist_ok=True)
    athletes.coalesce(1).write.parquet(os.path.join(output_dir, "athletes_combined.parquet"), mode='overwrite')

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usage: spark.py <athletes_2012.csv> <athletes_2016.csv> <athletes_2020.csv> <coaches.csv> <medals.csv> <output_dir>")
        sys.exit(1)
    main(sys.argv)
