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

rental_schema = StructType([
    StructField('rental_id', IntegerType(), False),
    StructField('rental_date', TimestampType(), False),
    StructField('inventory_id', IntegerType(), False),
    StructField('customer_id', IntegerType(), False),
    StructField('return_date', TimestampType(), False),
    StructField('staff_id', IntegerType(), False),
    StructField('last_update', TimestampType(), False),
])

inventory_schema = StructType([
    StructField('inventory_id', IntegerType(), False),
    StructField('film_id', IntegerType(), False),
    StructField('store_id', IntegerType(), False),
    StructField('last_update', TimestampType(), False),
])

actor_schema = StructType([
    StructField('actor_id', IntegerType(), False),
    StructField('first_name', StringType(), False),
    StructField('last_name', StringType(), False),
    StructField('last_update', TimestampType(), False),
])

film_actor_df = spark.read.csv('./data/film_actor.csv', header=True, schema=film_actor_schema)
rental_df = spark.read.csv('./data/rental.csv', header=True, schema=rental_schema)
inventory_df = spark.read.csv('./data/inventory.csv', header=True, schema=inventory_schema)
actor_df = spark.read.csv('./data/actor.csv', header=True, schema=actor_schema)

join1_df = rental_df.join(inventory_df, inventory_df.inventory_id == rental_df.inventory_id, 'inner')
join2_df = join1_df.join(film_actor_df, film_actor_df.film_id == inventory_df.film_id, 'inner')

grouped_df = join2_df.groupBy(film_actor_df.actor_id).agg(film_actor_df.actor_id,
                                                          F.count(rental_df.rental_id).alias('rental_count'))
grouped_df = grouped_df.orderBy(F.desc('rental_count')).limit(10).select('actor_id', 'rental_count')

top_actors_df = grouped_df.join(actor_df, actor_df.actor_id == grouped_df.actor_id).orderBy(
    F.desc(grouped_df.rental_count)).select(actor_df.actor_id, actor_df.first_name, actor_df.last_name,
                                            grouped_df.rental_count)

top_actors_df.show()
