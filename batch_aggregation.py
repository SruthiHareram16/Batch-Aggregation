from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, round, upper
import shutil


# Create a Spark session
spark = SparkSession.builder \
    .appName("Batch_aggregation.py") \
    .getOrCreate()
#Read CSV
df = spark.read.csv("./Input/batch_agg_input.csv", header=True, inferSchema=True)

#Formating
df = df.withColumn("measurement_type",upper(col("measurement_type"))).withColumn("date", to_date(col("timestamp")))
#df.show()

# Aggregating at day level
aggregated_df = df.groupBy(
    col("date"),
    col("measurement_type")
).agg(
    round(avg("value"),2).alias("average_value"),
    min("value").alias("Min Value for the Day"),
    max("value").alias("Max value for the Day")
).orderBy(col("date"),col("measurement_type"))

# result dataframe
#aggregated_df.show()

# writing to csv
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
aggregated_df.write.mode('overwrite').csv("./Output",header=True)

# renaming the file

for filename in os.listdir("./Output"):
    if filename.startswith("part-"):  # Check if it is a part file
        old_file_path = os.path.join("./Output", filename)
        new_file_path = os.path.join("./Output", "batch_agg_output.csv")

        # Move (rename) the file
        shutil.move(old_file_path, new_file_path)

# Stop the Spark session
spark.stop()