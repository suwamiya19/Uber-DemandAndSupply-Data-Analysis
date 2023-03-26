# Databricks notebook source
#importing pyspark libraries
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import *

# loading the data
uberdf = spark.read.csv("/FileStore/dataset.csv",header=True,inferSchema=True)

#replacing the nulls with previous know dates
uberdf=uberdf.withColumn("Counter",monotonically_increasing_id())
uberdf=uberdf.withColumn("Dates",when(uberdf.Date>1,uberdf.Date).otherwise(last(uberdf.Date,ignorenulls=True).over(Window.orderBy("Counter"))))
uberdf.drop("Counter")
#uberdf=uberdf.withColumn("Time (Local)",col("`Time (Local)`").cast(TimestampType()))
uberdf=uberdf.select(to_date(col("Dates"),"dd-MMM-yy").alias("Date"),hour(to_timestamp(col("Time (Local)")*3600)).alias("Time"),"`Eyeballs `","`Zeroes `","`Completed Trips `","`Requests `","`Unique Drivers`")
display(uberdf)

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Date had the most completed trips**

# COMMAND ----------

from pyspark.sql.functions import *

tripsByDate=uberdf.groupBy("Date").sum("Completed Trips ")
dateMostCompletedTrips = tripsByDate.orderBy("sum(Completed Trips )", ascending=False).select("Date").first()["Date"]
print(dateMostCompletedTrips)

# COMMAND ----------

# MAGIC %md
# MAGIC **2. The highest number of completed trips within a 24-hour period**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window

#max trips in a day
highestTrips=tripsByDate.orderBy("sum(Completed Trips )", ascending=False).select(col("sum(Completed Trips )").alias("MaxTrips")).first()["MaxTrips"]
print(highestTrips)

#max trips in rolling 24 hours period

'''
tripsby24hrwindow=uberdf.groupBy(window("Time","24 hours")).agg(sum("Completed Trips ").alias("CompletedTrips")).orderBy("CompletedTrips",ascending=False)
tripsby24hrwindow.show(truncate=False)

'''

# COMMAND ----------

# MAGIC %md
# MAGIC **Busiest Hour of the day (had the most requests)**

# COMMAND ----------

from pyspark.sql.functions import *

maxtripsbyhours = uberdf.groupBy(col("Time").alias("Hours")).agg(sum("Requests ").alias("No of requests"))
maxhour = maxtripsbyhours.orderBy("No of requests",ascending=False).select("Hours","No of requests")
maxhour.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Percentages of all zeroes occurred on weekends (Friday at 5 pm to Sunday at 3 am)**

# COMMAND ----------

from pyspark.sql.functions import *

weekendZeros = uberdf.filter((col("Time")>=17) & (dayofweek(col("Date"))==5)).agg(sum(col("Zeroes ")).alias("weekendZeros")).collect()[0]["weekendZeros"] + \
uberdf.filter((col("Time")<3) & (dayofweek(col("Date"))==7)).agg(sum(col("Zeroes ")).alias("weekendZeros")).collect()[0]["weekendZeros"]+\
uberdf.filter(dayofweek(col("Date"))==6).agg(sum(col("Zeroes ")).alias("weekendZeros")).collect()[0]["weekendZeros"]
print(weekendZeros)
weekZeros = uberdf.agg(sum(col("Zeroes ")).alias("weekZeros")).collect()[0]["weekZeros"]
print(weekZeros)


percentageWeekendZeros = weekendZeros/weekZeros * 100
print("Percentage of weeekend zeros",percentageWeekendZeros)

#weekendZeros = uberdf.where(dayofweek(col("Date"))==6 | dayofweek(col("Date"))==7).sum(col("Zeroes"))

# COMMAND ----------

# MAGIC %md
# MAGIC **The weighted average ratio of completed trips per driver**

# COMMAND ----------

from pyspark.sql.functions import *

weightedAvg = uberdf.withColumn("CompletedTripsbyDriver",uberdf["Completed Trips "]/uberdf["Unique Drivers"]).groupBy("Date","Time").agg(avg("CompletedTripsbyDriver").alias("Avg_CompletedTripsbyDriver"),sum("CompletedTripsbyDriver").alias("total_CompletedTripsbyDriver")).withColumn("WeightedRatio", col("Avg_CompletedTripsbyDriver") * col("total_CompletedTripsbyDriver")).agg(sum("WeightedRatio")/sum("total_CompletedTripsbyDriver")).collect()[0][0]
print(weightedAvg)

# COMMAND ----------

# MAGIC %md
# MAGIC **Time when to consider a true “end day” instead of midnight (i.e when are supply and demand at both their natural minimums)**

# COMMAND ----------

from pyspark.sql.functions import *

endDayHour = uberdf.groupBy("Time").agg(avg("Requests ").alias("Requests"),avg("Unique Drivers").alias("Drivers")).orderBy("Requests","Drivers",ascending=True).first()["Time"]
print(endDayHour)

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding more drivers to any single hour of every day based on demand**

# COMMAND ----------

from pyspark.sql.functions import *

moreDriverHour = uberdf.groupBy("Time").agg((sum("requests ")/countDistinct("Unique Drivers")).alias("reqperdriver")).orderBy("reqperdriver",ascendening=True).first()["Time"]
print(moreDriverHour)
