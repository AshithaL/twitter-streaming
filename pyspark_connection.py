from pyspark.sql import SparkSession
from pyspark.sql import functions

spark = SparkSession.builder.getOrCreate()
mysql_df = spark.read.format("jdbc").options(
    url="jdbc:mysql://127.0.0.1:3306/twitter_analysis?characterEncoding=UTF-8&autoReconnect=true",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "tweet",
    user="root",
    password="nineleaps").load()
mysql_df.show(100)