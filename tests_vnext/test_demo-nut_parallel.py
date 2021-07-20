# Databricks notebook source
# Parallel test with Nutter

# COMMAND ----------

from runtime.nutterfixture import NutterFixture
from runtime.runner import NutterFixtureParallelRunner

# COMMAND ----------

class MyDemoFixture(NutterFixture):
  def run_test_name(self):
    dbutils.notebook.run('../notebooks/demo-nut', 600, {'source_table': 'avocado_csv'})
      
  def assertion_test_name(self):
    # Row count
    tbl_count = sqlContext.sql('SELECT COUNT(*) FROM global_temp.avocado_csv_preproc').first()
    assert tbl_count[0] == 18249
    # Schema
    tbl_schema = sqlContext.sql('DESCRIBE TABLE global_temp.avocado_csv_preproc')
    assert tbl_schema.count() == 9

# COMMAND ----------

runner = NutterFixtureParallelRunner(4)
for i in range(8):
  runner.add_test_fixture(MyDemoFixture())

# COMMAND ----------

result = runner.execute()

# COMMAND ----------

print(result.to_string())
