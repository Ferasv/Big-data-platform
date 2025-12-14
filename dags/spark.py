import sys, os
import psycopg2
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from datetime import datetime, timedelta
import time
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import clickhouse_connect



class HDFS:
    def __init__(self, hdfs_url="hdfs://192.168.126.116:9000", hdfs_base="/kafka_message",host= '192.168.126.51',port=8123,password=''):
        self.hdfs_base = hdfs_base
        self.hdfs_url = hdfs_url
        self.spark = None  # SparkSession will be created later
       
  

    # Create Spark session when needed
    def start_spark(self):
        from pyspark.sql import SparkSession  # Import inside function
        try:
            self.spark = SparkSession.builder \
            .appName("hdfs-Hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.126.116:9000") \
            .config("hive.metastore.uris", "thrift://remote-metastore-host:90 3") \
            .enableHiveSupport() \
            .getOrCreate()
            print("✅ Spark session started successfully!")
            print(self.spark.conf.get("spark.sql.catalog.clickhouse.compress"))

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
            df = self.spark.read \
            .schema(schema) \
            .json("hdfs://192.168.126.116:9000/kafka_message")

            print(f"📂 Reading from HDFS path: {data_path}")
            df.show() 
            

            return df
        except AnalysisException:
            print(f"⚠️ No data found in {data_path}")
            return None



    # Transform and filter last 2 minutes
    def transform_data(self, df):
        if df is None:
            return None
        df_agg = df.withColumn("wallet", F.col("wallet").cast(DoubleType())).agg(F.avg("wallet").alias("avg_wallet"))
        return df_agg
    
    
    
    
    

        
        
        
        
    def add_data_clickhouse(self, df, table_name="user"):
        if df is None:
            print("❌ No data to insert into ClickHouse.")
            return None
        try:
            df.writeTo(f"clickhouse.default.{table_name}") \
            .append()
            print(f"✅ Data inserted into ClickHouse table '{table_name}' successfully!")
        except Exception as e:
            print("❌ Error inserting data into ClickHouse:", e)
        
    def stop_spark(self):
        if self.spark is not None:
            self.spark.stop()

   

    # Example main function (can be used in script, not DAG import)
    def main(self):

        self.start_spark()
        df = self.spark_read_hdfs()
        df = self.transform_data(df)
        # self.stop_spark()
        
        self.add_data_clickhouse(df)
        
if __name__ == "__main__":
    hdfs_handler = HDFS()
    hdfs_handler.check_connection_clickhouse()
    hdfs_handler.main()

