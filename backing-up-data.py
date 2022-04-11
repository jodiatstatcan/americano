# Databricks notebook source
# MAGIC %md
# MAGIC # Backing Up Data in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. [deep clone entire database](https://medium.com/@bhaveshpatelaus/backing-up-full-delta-lake-using-delta-deep-clone-and-dynamic-python-spark-sql-scripting-in-a7c6a820607c)
# MAGIC 2. [external tables for version control](https://www.adaltas.com/en/2020/05/21/data-import-databricks/)
# MAGIC 3. [control data location](https://www.projectpro.io/recipes/control-data-location-while-creating-delta-tables-databricks)
# MAGIC 
# MAGIC Unmanaged tables' files are not deleted when you DROP the table, may be suitable for version control/back up? Generate an unmanaged table every quarter for backup, but it doesn't have version control according to 2. Benefit is that it can just be like a "snapshot" of vpop data we used to generate the numbers that quarter. Alternatively if we clone entire database of Delta managed tables, will all older versions be kept? 

# COMMAND ----------

# MAGIC %md
# MAGIC Deep clone test database

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Target Database
# MAGIC CREATE SCHEMA IF NOT EXISTS test_cloned;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get location of database by extended descriptions of a table in the source db
# MAGIC DESCRIBE TABLE EXTENDED test.random

# COMMAND ----------

# %sql
# -- Drop Cloned
# DROP SCHEMA IF EXISTS test_cloned;

# COMMAND ----------

# Check tables in source database
source_tables = spark.sql("""SHOW TABLES IN test""")
display(source_tables)

# COMMAND ----------

# List Variable to store all table names in source database
tableList = [x["tableName"] for x in source_tables.rdd.collect()]
# SQL deep clone
for tbl in tableList:
  spark.sql(f"CREATE OR REPLACE TABLE test_cloned.{tbl} DEEP CLONE test.{tbl} LOCATION 'dbfs:/user/hive/warehouse/test.db/{tbl}'") # clone is only supported for delta tables
  print(f"test_cloned.{tbl} is successfully backed up")

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE test_cloned.random DEEP CLONE test.random LOCATION 'dbfs:/user/hive/warehouse/test.db/random'")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY test_cloned.random

# COMMAND ----------

# MAGIC %md
# MAGIC External tables
# MAGIC 
# MAGIC Kind of confusing honestly, probably not as great of an option to use for backing up data. Cloning database is preferred.

# COMMAND ----------

# test with particular table
random_sdf = spark.table("test.random")

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/'))

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/vrf/test
# MAGIC 
# MAGIC # will make external tables here

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.unmanaged_random;
