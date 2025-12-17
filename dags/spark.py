import sys, os
import psycopg2
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from datetime import datetime, timedelta
import time
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import clickhouse_connect
import json

from pyspark.sql.types import (
StructType, StructField,
StringType, DoubleType, IntegerType, LongType, BooleanType
)



class HDFS:
    def __init__(self, hdfs_url="hdfs://192.168.126.116:9000", hdfs_base="/data_user",host= '192.168.126.51',port=8123,password=''):
        self.hdfs_base = hdfs_base
        self.hdfs_url = hdfs_url
        self.spark = None  # SparkSession will be created later
       
  

    # Create Spark session when needed
    def start_spark(self):
        from pyspark.sql import SparkSession  # Import inside function
        try:
            self.spark = SparkSession.builder \
                .appName("hdfs-to-hive") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.126.116:9000") \
                .config("spark.sql.warehouse.dir", "hdfs://192.168.126.116:9000/user/hive/warehouse") \
                .config("hive.metastore.uris", "thrift://192.168.126.116:9083") \
                .config("HADOOP_USER_NAME", "hadoop_cluster") \
                .enableHiveSupport() \
                .getOrCreate()

            self.spark.sparkContext._jsc.hadoopConfiguration().set("HADOOP_USER_NAME", "hadoop_cluster")
            print("✅ Spark session started successfully!")
        
            

        except Exception as e:
            print("❌ Spark initialization error:", e)
            self.spark = None


       # Read HDFS JSON files
    def spark_read_hdfs(self,type):
        with open("/home/wakeb/Desktop/docker_project_pipeline/schema_data.json") as f:
            schema = json.load(f)[type]
        print(f" wow Loaded schema for {type}: {schema}")
        schema = self.json_schema_to_spark(schema)
        
       
        if not self.spark:
            print("❌ Spark session is not initialized.")
            return None
        data_path = f"{self.hdfs_base}/*.json"
        try:
            df = self.spark.read \
            .schema(schema) \
            .json(f"hdfs://192.168.126.116:9000/data_user/{type}")

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
    
    
    
    
   


    def json_schema_to_spark(self, json_schema: dict) -> StructType:
            TYPE_MAP = {
            "string": StringType(),
            "double": DoubleType(),
            "int": IntegerType(),
            "long": LongType(),
            "boolean": BooleanType()}
            
            fields = [
                StructField(name, TYPE_MAP[dtype], True)
                for name, dtype in json_schema.items()
            ]
            return StructType(fields)
    
    
    
    def verify_hive_connection(self):
        if not self.spark:
            print("❌ Spark session is not initialized.")
            return False
        try:
            self.spark.sql("SHOW DATABASES").show()
            print(self.spark.conf.get("spark.sql.warehouse.dir"))

            self.spark.sql("SHOW TABLES IN jahez_db").show()
            print("✅ Hive connection verified successfully!")
            return True
        except Exception as e:
            print("❌ Hive connection error:", e)
            return False
    

        
        
        
        
    def insert_data_hive(self, df, table_name="user"):
        if df is None:
            print("❌ No data to insert into Hive.")
            return None
        try:
            df.writeTo(f"hive.default.{table_name}") \
            .append()
            print(f"✅ Data inserted into Hive table '{table_name}' successfully!")
        except Exception as e:
            print("❌ Error inserting data into Hive:", e)
        
    def stop_spark(self):
        if self.spark is not None:
            self.spark.stop()
            


            

    def load_into_hive(self, df, database, table):
        if df is None:
            print("❌ No data to load into Hive.")
            return

        try:
            full_table = f"{database}.{table}"
            df.write.mode("append").insertInto(full_table)
            print(f"✅ Data loaded into Hive table {full_table}")
        except Exception as e:
            print("❌ Hive load error:", e)


   

    # Example main function (can be used in script, not DAG import)
    def main(self):

        self.start_spark()
        # self.verify_hive_connection()
        df = self.spark_read_hdfs('customer')
        # df = self.transform_data(df)
        # self.stop_spark()
        
        
        
if __name__ == "__main__":
    hdfs_handler = HDFS()
    hdfs_handler.main()

