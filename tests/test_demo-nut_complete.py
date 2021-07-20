# Databricks notebook source
# More complete test with Nutter

# COMMAND ----------

import json

from runtime.nutterfixture import NutterFixture

# COMMAND ----------

class MyDemoFixture(NutterFixture):

  # Constructor
  
  def __init__(self):
    self.expected_tbl_name = "temp_test_table_preproc"
    super().__init__()

  # Setup
  
  def before_all(self):
    sqlContext.sql('SELECT * FROM avocado_csv LIMIT 1000').createOrReplaceGlobalTempView("temp_test_table")
  
  # Simple test
  
  def run_simple_test(self):
    res = dbutils.notebook.run('../notebooks/demo-nut', 600, {'source_table': 'global_temp.temp_test_table'})
    self.simple_result = json.loads(res)
      
  def assertion_simple_test(self):
    # Destination table name
    
    assert self.simple_result['table'] == self.expected_tbl_name
    # Row count
    tbl_count = sqlContext.sql('SELECT COUNT(*) FROM global_temp.' + self.expected_tbl_name).first()
    assert tbl_count[0] == 1000
    # Schema
    tbl_schema = sqlContext.sql('DESCRIBE TABLE global_temp.' + self.expected_tbl_name)
    assert tbl_schema.count() == 9

  # Another test
  
  def before_another_test(self):
    sqlContext.sql('SELECT DISTINCT(region) FROM avocado_csv').createOrReplaceGlobalTempView("another_temp_test_table")
    pass
  
  def run_another_test(self):
    res = dbutils.notebook.run('../notebooks/demo-nut', 600, {'source_table': 'global_temp.another_temp_test_table'})
    self.another_result = json.loads(res)
      
  def assertion_another_test(self):
    # The region column should contain no lower case characters
    tbl_lower = sqlContext.sql('SELECT COUNT(*) FROM global_temp.another_temp_test_table_preproc WHERE region RLIKE "[a-z]+"').first()
    assert tbl_lower[0] == 0
  
  def after_another_test(self):
    sqlContext.sql('DROP TABLE global_temp.another_temp_test_table')
    sqlContext.sql('DROP TABLE global_temp.another_temp_test_table_preproc')

  # Cleanup
  
  def after_all(self):
    sqlContext.sql('DROP TABLE global_temp.temp_test_table_preproc')

# COMMAND ----------

test = MyDemoFixture()

# COMMAND ----------

result = test.execute_tests()

# COMMAND ----------

print(result.to_string())

# COMMAND ----------

result.exit(dbutils)

# COMMAND ----------


