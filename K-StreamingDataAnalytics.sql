-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Fitness Tracker: Streaming Data Analytics
-- MAGIC * [Create Events](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#notebook/1498933687658160)
-- MAGIC * [DLT Pipeline](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#joblist/pipelines/5625812f-b4f5-40a2-8807-794c5f1afa33/updates/9cfe4782-1e5b-4575-b962-e28585cdd9fd)
-- MAGIC * [Data Donation Project](https://corona-datenspende.de/science/en/reports/longcovidlaunch/)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Global Statistics

-- COMMAND ----------

select *  from heartcc.global_stat

-- COMMAND ----------

select count(*)  from heartcc.bpm_cleansed

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Streaming Data Analytics 
-- MAGIC streaming data: BPM aggregated over 1 minute [tumbling window](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(spark.readStream.format("delta").table("heart.bpm_cleansed").
-- MAGIC         groupBy("bpm",window("time", "5 minute")).avg("bpm").orderBy("window",ascending=True))

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC The data donation project: https://corona-datenspende.de/science/en/reports/longcovidlaunch/ 
