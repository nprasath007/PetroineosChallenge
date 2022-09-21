from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
from datetime import datetime

spark.sql("set spark.databricks.userInfoFunctions.enabled = true");
spark.sql("set spark.databricks.delta.schema.autoMerge = true");

Positions_location = "/FileStore/tables/Positions.csv"
Time_location = "/FileStore/tables/Time.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# read data from csv to spark df
positions_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(Positions_location)

# read data from csv to spark df
time_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(Time_location)

# Join Positions to Time Period to get the Time in HH:MM format
sdf = positions_df.join(time_df, positions_df.Period==time_df.Period, "inner").drop(positions_df.Period)

# declare object for the Class 'Positions'
Positions_obj = Positions(sdf,spark)

target_path = 'dbfs:/FileStore/df/'
audit_path = 'dbfs:/FileStore/df/'

# Run the GetTrades and AuditLogs Methods
Positions_obj.GetTrades('01/04/2015',target_path)
Positions_obj.AuditLogs('01/04/2015',audit_path)