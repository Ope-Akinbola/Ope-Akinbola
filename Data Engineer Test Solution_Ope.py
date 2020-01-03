# Databricks notebook source
# MAGIC %md
# MAGIC The first step is to spark session and all spark SQL functionalities

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
import pyspark.sql.types as typ

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC Import the csv files and creating a dataframe for the two data

# COMMAND ----------

weather = spark.read.csv("/FileStore/tables/weather_20160301-75f39.csv", header = True)
weatherDF = spark.read.csv("/FileStore/tables/weather_20160201-f2907.csv", header = True)

# COMMAND ----------

weather.createOrReplaceTempView("weather")
weatherDF.createOrReplaceTempView("weather2")

# COMMAND ----------

# MAGIC %md
# MAGIC Converting the csv files into parquets files

# COMMAND ----------

weather = spark.read.csv("/FileStore/tables/weather_20160301-75f39.csv", header = True)
weather2 = spark.read.csv("/FileStore/tables/weather_20160201-f2907.csv", header = True)
weather.write.mode("overwrite").parquet("/FileStore/tables/weather_20160301-75f39.parquet")
weather2.write.mode("overwrite").parquet("/FileStore/tables/weather_20160201-f2907.parquet")

# COMMAND ----------

weather.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Joining the two weather tables together in a dataframe (weatherData) using the union command 

# COMMAND ----------

weatherData = weather.union(weather2)

# COMMAND ----------

display(weatherData)

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a temporary view table "weatherData" to be queryable

# COMMAND ----------

weatherData.createOrReplaceTempView('weatherData')

# COMMAND ----------

# MAGIC %md
# MAGIC Querying the weatherData to solve the solutions

# COMMAND ----------

spark.sql("SELECT Region,ObservationDate,ScreenTemperature FROM weatherData WHERE ScreenTemperature IN (SELECT MAX(ScreenTemperature) FROM data)").show()
