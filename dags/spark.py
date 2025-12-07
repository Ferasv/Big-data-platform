import sys, os
import psycopg2
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from datetime import datetime, timedelta
import time
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

class HDFS:
    def __init__(self, hdfs_url="hdfs://192.168.126.116:9000", hdfs_base="/kafka_message"):
        self.hdfs_base = hdfs_base
        self.hdfs_url = hdfs_url
        self.spark = None  # SparkSession will be created later
        

  

    # Create Spark session when needed
    def start_spark(self):
        from pyspark.sql import SparkSession  # Import inside function
        try:
            self.spark = SparkSession.builder \
                .appName("HDFSReader") \
                .config("spark.hadoop.fs.defaultFS", self.hdfs_url) \
                .getOrCreate()
            print("✅ Spark session started successfully!")
        except Exception as e:
            print("❌ Spark initialization error:", e)
            self.spark = None
            
            
       
    # Read HDFS JSON files
    def spark_read_hdfs(self):
        schema = StructType() \
        .add("name", StringType()) \
        .add("phone", StringType()) \
        .add("wallet", DoubleType())
        if not self.spark:
            print("❌ Spark session is not initialized.")
            return None
        data_path = f"{self.hdfs_base}/*.json"
        try:
            df = self.spark.readStream \
            .schema(schema) \
            .json("hdfs://192.168.126.116:9000/kafka_message")

            print(f"📂 Reading from HDFS path: {data_path}")
            return df
        except AnalysisException:
            print(f"⚠️ No data found in {data_path}")
            return None



    # Transform and filter last 2 minutes
    def transform_data(self, df):
        if df is None:
            return None
        two_min_ago = datetime.now() - timedelta(minutes=2)
        df_filtered = df.filter(F.col("timestamp") >= F.lit(two_min_ago))
        transformed_df = df_filtered.select("registered_date", "first_name", "last_name",
                                            "email", "username", "phone", "timestamp")
        transformed_df = transformed_df.withColumn("registered_date", F.to_timestamp("registered_date"))
        transformed_df = transformed_df.filter(F.col("email").isNotNull())
        return transformed_df

   

    # Example main function (can be used in script, not DAG import)
    def main(self):
        self.start_spark()
        df = self.spark_read_hdfs()
        # df = self.transform_data(df)
        
if __name__ == "__main__":
    hdfs_handler = HDFS()
    hdfs_handler.main()

