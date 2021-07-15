# Databricks notebook source
# Simple test with Nutter

# COMMAND ----------

import json

from runtime.nutterfixture import NutterFixture

# COMMAND ----------

class MyDemoFixture(NutterFixture):
  def run_simple_test(self):
    res = dbutils.notebook.run('../notebooks/demo-nut', 600, {'source_table': 'avocado_csv'})
    self.result = json.loads(res)
      
  def assertion_simple_test(self):
    # Destination table name
    expected_tbl_name = "avocado_csv_preproc"
    assert self.result['table'] == expected_tbl_name
    # Row count
    tbl_count = sqlContext.sql('SELECT COUNT(*) FROM global_temp.' + expected_tbl_name).first()
    assert tbl_count[0] == 18249
    # Schema
    tbl_schema = sqlContext.sql('DESCRIBE TABLE global_temp.' + expected_tbl_name)
    assert tbl_schema.count() == 9

# COMMAND ----------

test = MyDemoFixture()

# COMMAND ----------

result = test.execute_tests()

# COMMAND ----------

print(result.to_string())

# COMMAND ----------

result.exit(dbutils)
