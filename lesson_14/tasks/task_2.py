from pyspark.sql import SparkSession, functions as F

import schemas

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DataFrameAPIExample") \
    .getOrCreate()

film_actor_df = spark.read.csv('./data/film_actor.csv', header=True, schema=schemas.film_actor_schema)
rental_df = spark.read.csv('./data/rental.csv', header=True, schema=schemas.rental_schema)
inventory_df = spark.read.csv('./data/inventory.csv', header=True, schema=schemas.inventory_schema)
actor_df = spark.read.csv('./data/actor.csv', header=True, schema=schemas.actor_schema)

join1_df = rental_df.join(inventory_df, inventory_df.inventory_id == rental_df.inventory_id, 'inner')
join2_df = join1_df.join(film_actor_df, film_actor_df.film_id == inventory_df.film_id, 'inner')

grouped_df = join2_df.groupBy(film_actor_df.actor_id).agg(film_actor_df.actor_id,
                                                          F.count(rental_df.rental_id).alias('rental_count'))
ordered_df = grouped_df.orderBy(F.desc('rental_count')).limit(10).select('actor_id', 'rental_count')

top_actors_df = ordered_df.join(actor_df, actor_df.actor_id == ordered_df.actor_id).orderBy(
    F.desc(ordered_df.rental_count)).select(actor_df.actor_id, actor_df.first_name, actor_df.last_name,
                                            ordered_df.rental_count)

top_actors_df.show()
