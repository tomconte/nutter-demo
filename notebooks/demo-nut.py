# Databricks notebook source
# This is a demo Notebook Under Test

# COMMAND ----------

dbutils.widgets.text('source_table', '')

# COMMAND ----------

source_table = dbutils.widgets.get('source_table')
df = sqlContext.sql('SELECT * FROM ' + source_table)
df.printSchema()

# COMMAND ----------

# Do something with the data

from pyspark.sql.functions import upper

df = df.withColumnRenamed('Date', 'date')\
       .withColumnRenamed('AveragePrice', 'average_price')\
       .withColumnRenamed('Total Volume', 'total_volume')\
       .drop('_c0', 'Total Bags', 'Small Bags', 'Large Bags', 'XLarge Bags')

# TODO: do more stuff eventually

df = df.withColumn('region', upper('region'))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

dest_table = source_table + '_preproc'
dest_table = dest_table.split('.')[-1] # Keep name after dot
df.createOrReplaceGlobalTempView(dest_table)

# COMMAND ----------

# Return results

import json

dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "table": dest_table
}))

# COMMAND ----------


