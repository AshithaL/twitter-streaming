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


#Location wise most popular hashtags
dataframe_mysql = mysql_df.withColumn("hashtag",functions.low(functions.col("hashtag")))
feature_group = ['location','hashtag']
data_counts = dataframe_mysql.groupBy(['location','hashtag']).agg(functions.count('hashtag').alias('count'))
data_joined = dataframe_mysql.join(data_counts, feature_group)
x = data_joined.select(['location','username','hashtag']).sort('location',ascending = False).distinct()
y = x.groupBy(['location','hashtag']).count().orderBy('count',ascending=False)
y.show()

# For the same hashtags, find out the per hour frequency of tweet for each location.
dataframe_mysql = dataframe_mysql.withColumn("location",functions.lower(functions.col("location")))
tweet_frequency = dataframe_mysql.groupBy([functions.hour("time").alias("hour"),'location']).agg(functions.count("tweet_data").alias("count"))
tweet_frequency.sort('location',ascending=False).show()