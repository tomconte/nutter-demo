# Databricks notebook source
# Simple test with Nutter

# COMMAND ----------

from runtime.nutterfixture import NutterFixture

# COMMAND ----------

class MyDemoFixture(NutterFixture):
  def run_simple_test(self):
    dbutils.notebook.run('demo-nut', 600, {'source_table': 'avocado_csv'})
      
  def assertion_simple_test(self):
    # Row count
    tbl_count = sqlContext.sql('SELECT COUNT(*) FROM global_temp.nut_demo').first()
    assert tbl_count[0] == 18249
    # Schema
    tbl_schema = sqlContext.sql('DESCRIBE TABLE global_temp.nut_demo')
    assert tbl_schema.count() == 9

# COMMAND ----------

test = MyDemoFixture()

# COMMAND ----------

result = test.execute_tests()

# COMMAND ----------

print(result.to_string())

# COMMAND ----------


