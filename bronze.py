# Databricks notebook source
#for test 
display(df_shows_data.limit(10))
display(df_episodes_data.limit(10))
display(df_cast_data.limit(10))

# COMMAND ----------

df_shows = spark.read.json(f"{bronze_path}/shows")
df_shows.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"bronze_abn.shows")

df_episodes = spark.read.json(f"{bronze_path}/episodes")
df_episodes.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"bronze_abn.episodes")

df_cast = spark.read.json(f"{bronze_path}/cast")
df_cast.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"bronze_abn.cast")