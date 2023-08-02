from databricks.connect import DatabricksSession as SparkSession
from databricks.sdk.core import Config
# from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
# c = Config(profile="DEFAULT", cluster_id="0801-073038-t64kumcx")
# spark = SparkSession.builder.sdkConfig(c).getOrCreate()

schema = StructType([
  StructField('CustomerID', IntegerType(), False),
  StructField('FirstName',  StringType(),  False),
  StructField('LastName',   StringType(),  False)
])

data = [
  [ 1000, 'Mathijs', 'Oosterhout-Rijntjes' ],
  [ 1001, 'Joost',   'van Brunswijk' ],
  [ 1002, 'Stan',    'Bokenkamp' ]
]

customers = spark.createDataFrame(data, schema)
customers.show()

# Output:
#
# +----------+---------+-------------------+
# |CustomerID|FirstName|           LastName|
# +----------+---------+-------------------+
# |      1000|  Mathijs|Oosterhout-Rijntjes|
# |      1001|    Joost|      van Brunswijk|
# |      1002|     Stan|          Bokenkamp|
# +----------+---------+-------------------+