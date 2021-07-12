# Databricks notebook source
# This is a demo Notebook Under Test

# COMMAND ----------

dbutils.widgets.text('source_table', '')

# COMMAND ----------

df = sqlContext.sql('SELECT * FROM ' + dbutils.widgets.get('source_table') + ' LIMIT 1000')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Do something with the data

df = df.withColumnRenamed('Date', 'date')\
       .withColumnRenamed('AveragePrice', 'average_price')\
       .withColumnRenamed('Total Volume', 'total_volume')\
       .drop('_c0', 'Total Bags', 'Small Bags', 'Large Bags', 'XLarge Bags')

# TODO: do more stuff eventually

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceGlobalTempView('nut_demo')
