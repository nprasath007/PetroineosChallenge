"""
@author: prasath.natarajan
"""
# from pyspark.sql import functions as F, Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType,DateType,LongType, DoubleType
from pyspark.sql.session import SparkSession
from datetime import datetime, time
from dateutil.parser import parse

now = datetime.now()

class Positions:
    def __init__(self,sdf,spark):
        self.sdf = sdf
        self.spark = spark
   
    def GetTrades(self, date, location):
      try:
        parse(date)
      except:
        print('Gettrades Input Date is not valid')
        return False
      try:
        currentdatetime = self.spark.sql("select current_date()")
        current = datetime.now()
        date_time = current.strftime("%Y%m%d_%H%M")
        temp_df = self.sdf.groupBy('Date','Period','Time').agg(sum('Volume').alias('Volume')).filter( \
                        col('Date') == date).drop('Date')
        temp_df = temp_df.select('Time','Volume').sort(col('Period').cast("int").asc())
        temp_df.coalesce(1).write.mode("overwrite").format("csv").option("header","true").save(location+"PowerPosition_"+date_time+".csv")
        temp_df.coalesce(1).write.format("delta").mode("append").option("header","true").save(location+"PowerPosition")
        print("Trades csv uploaded successfully")
      except:
        return 'Error: in GetTrades function'
      
    def AuditLogs(self, date, location):
      try:
        parse(date)
      except:
        return 'Auditlogs Input Date is not valid'
      try:
        audit_df = self.spark.sql("select current_timestamp() as load_datetime, \
        current_user() as user_name")
        total_count = self.sdf.filter(col('Date') == date).count()   
        audit_df = audit_df.withColumn("Position_date",lit(date))
        audit_df = audit_df.withColumn("Total_rows",lit(total_count))
        audit_df.coalesce(1).write.format("delta").mode("append").option("mergeSchema", \
                   "true").option("header","true").save(location+"Auditlogs_PowerPosition")
        print("Audits delta uploaded successfully")
      except:
          return 'Error: in Audit function'