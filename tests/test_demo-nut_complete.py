# Databricks notebook source
# Simple test with Nutter

# COMMAND ----------

from runtime.nutterfixture import NutterFixture

# COMMAND ----------

class MyDemoFixture(NutterFixture):
  def before_all(self):
    sqlContext.sql('SELECT * FROM avocado_csv LIMIT 1000').createOrReplaceGlobalTempView("temp_test_table")
  
  def run_simple_test(self):
    dbutils.notebook.run('../notebooks/demo-nut', 600, {'source_table': 'global_temp.temp_test_table'})
      
  def assertion_simple_test(self):
    # Row count
    tbl_count = sqlContext.sql('SELECT COUNT(*) FROM global_temp.nut_demo').first()
    assert tbl_count[0] == 1000
    # Schema
    tbl_schema = sqlContext.sql('DESCRIBE TABLE global_temp.nut_demo')
    assert tbl_schema.count() == 9

  def before_another_test(self):
    # Create some data frames on the fly?
    pass
  
  def run_another_test(self):
    dbutils.notebook.run('../notebooks/demo-nut', 600, {'source_table': 'global_temp.temp_test_table'})
      
  def assertion_another_test(self):
    # Row count
    tbl_count = sqlContext.sql('SELECT COUNT(*) FROM global_temp.nut_demo').first()
    assert tbl_count[0] == 1000
    # Schema
    tbl_schema = sqlContext.sql('DESCRIBE TABLE global_temp.nut_demo')
    assert tbl_schema.count() == 9

  def after_all(self):
    pass

# COMMAND ----------

test = MyDemoFixture()

# COMMAND ----------

result = test.execute_tests()

# COMMAND ----------

print(result.to_string())

# COMMAND ----------


