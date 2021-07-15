# Databricks notebook source
# Simple test with Nutter

# COMMAND ----------

from runtime.nutterfixture import NutterFixture
from runtime.runner import NutterRunner

# COMMAND ----------

class MyDemoFixture(NutterFixture):
  def run_test_name(self):
    dbutils.notebook.run('../notebooks/demo-nut', 600, {'source_table': 'avocado_csv'})
      
  def assertion_test_name(self):
    # Row count
    tbl_count = sqlContext.sql('SELECT COUNT(*) FROM global_temp.nut_demo').first()
    assert tbl_count[0] == 18249
    # Schema
    tbl_schema = sqlContext.sql('DESCRIBE TABLE global_temp.nut_demo')
    assert tbl_schema.count() == 9

# COMMAND ----------

tests = [
  MyDemoFixture(),
  MyDemoFixture(),
  MyDemoFixture(),
  MyDemoFixture(),
  MyDemoFixture(),
  MyDemoFixture(),
  MyDemoFixture(),
  MyDemoFixture()
]

# COMMAND ----------

runner = NutterRunner(tests, 4)

# COMMAND ----------

result = runner.execute_tests()

# COMMAND ----------

print(result.to_string())

# COMMAND ----------


