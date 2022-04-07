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
