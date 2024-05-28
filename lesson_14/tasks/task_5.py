from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DataFrameAPIExample") \
    .getOrCreate()

film_actor_schema = StructType([
    StructField('actor_id', IntegerType(), False),
    StructField('film_id', IntegerType(), False),
    StructField('last_update', TimestampType(), False),
])

actor_schema = StructType([
    StructField('actor_id', IntegerType(), False),
    StructField('first_name', StringType(), False),
    StructField('last_name', StringType(), False),
    StructField('last_update', TimestampType(), False),
])

film_category_schema = StructType([
    StructField('film_id', IntegerType(), False),
    StructField('category_id', IntegerType(), False),
    StructField('last_update', TimestampType(), False),
])

category_schema = StructType([
    StructField('category_id', IntegerType(), False),
    StructField('name', StringType(), False),
    StructField('last_update', TimestampType(), False),
])

film_actor_df = spark.read.csv('./data/film_actor.csv', header=True, schema=film_actor_schema)
actor_df = spark.read.csv('./data/actor.csv', header=True, schema=actor_schema)
film_category_df = spark.read.csv('./data/film_category.csv', header=True, schema=film_category_schema)
category_df = spark.read.csv('./data/category.csv', header=True, schema=category_schema)

join1_df = film_actor_df.join(actor_df, actor_df.actor_id == film_actor_df.actor_id, 'left')
join2_df = join1_df.join(film_category_df, film_actor_df.film_id == film_category_df.film_id, 'inner')
join3_df = join2_df.join(category_df, category_df.category_id == film_category_df.category_id, 'inner')

join3_df.filter(category_df.name == 'Children')

grouped_df = join3_df.groupBy(film_actor_df.actor_id, actor_df.first_name, actor_df.last_name).agg(
    F.sum(film_actor_df.actor_id).alias('appearance_count'))

sorted_df = grouped_df.orderBy(F.desc('appearance_count'))
sorted_df.select('first_name', 'last_name', 'appearance_count')

limited_df = sorted_df.limit(3)
limited_df.show()
