# Databricks notebook source
import requests
import json
base_path=f"abfss://sourcedata@abndemostorage.dfs.core.windows.net"
bronze_path=f"{base_path}/bronze/raw"
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------


def fetch_shows():
    url = "https://api.tvmaze.com/shows"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()
shows_data = fetch_shows()

def fetch_episodes(show_id):
    url = f"https://api.tvmaze.com/shows/{show_id}/episodes"
    response = requests.get(url)
    return response.json()

def fetch_cast(show_id):
    url = f"https://api.tvmaze.com/shows/{show_id}/cast"
    response = requests.get(url)
    return response.json()

episodes_data=[]
cast_data=[]
for show in shows_data:
    show_id = show["id"]
    try:
        episodes_data.extend(fetch_episodes(show_id))
        cast_data.extend(fetch_cast(show_id))
    except Exception as e:
        print(f"Error fetching episodes for show {show_id}: {e}")

# COMMAND ----------

## Errorcode 
#df_shows_data = spark.createDataFrame(shows_data)
#df_episodes_data = spark.createDataFrame(episodes_data)
#df_cast_data = spark.createDataFrame(cast_data)

df_shows_data = spark.read.json(spark.sparkContext.parallelize(shows_data))
df_episodes_data = spark.read.json(spark.sparkContext.parallelize(episodes_data))
df_cast_data = spark.read.json(spark.sparkContext.parallelize(cast_data))

# COMMAND ----------

# MAGIC %skip
# MAGIC import tempfile
# MAGIC import os
# MAGIC
# MAGIC def write_json_lines(data, name):
# MAGIC     temp_dir = tempfile.gettempdir()
# MAGIC     path = os.path.join(temp_dir, f"{name}.jsonl")
# MAGIC     with open(path, 'w') as f:
# MAGIC         for item in data:
# MAGIC             f.write(json.dumps(item) + '\n')
# MAGIC     return f"file:{path}"
# MAGIC
# MAGIC df_shows_data = spark.read.json(write_json_lines(shows_data, "shows"))
# MAGIC df_episodes_data = spark.read.json(write_json_lines(episodes_data, "episodes"))
# MAGIC df_cast_data = spark.read.json(write_json_lines(cast_data, "cast"))

# COMMAND ----------

#test
spark.conf.set(
  "fs.azure.account.key.abndemostorage.dfs.core.windows.net",
  "E03bAtGmc8RiJopddmUAmnuLk2+a0ITyIZu1I+V4wuscQrbPrTt3oUyHPXJf2XpcA4YL2vH1yqJP+AStde4D2w=="
)

# COMMAND ----------

# MAGIC %skip
# MAGIC #test
# MAGIC dbutils.fs.ls("abfss://sourcedata@abndemostorage.dfs.core.windows.net/")

# COMMAND ----------

df_shows_data.write.mode("overwrite").json(f"{bronze_path}/shows")
df_episodes_data.write.mode("overwrite").json(f"{bronze_path}/episodes")
df_cast_data.write.mode("overwrite").json(f"{bronze_path}/cast")
