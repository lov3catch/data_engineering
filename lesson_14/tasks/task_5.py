from pyspark.sql import SparkSession, functions as F

import schemas

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DataFrameAPIExample") \
    .getOrCreate()

film_actor_df = spark.read.csv('./data/film_actor.csv', header=True, schema=schemas.film_actor_schema)
actor_df = spark.read.csv('./data/actor.csv', header=True, schema=schemas.actor_schema)
film_category_df = spark.read.csv('./data/film_category.csv', header=True, schema=schemas.film_category_schema)
category_df = spark.read.csv('./data/category.csv', header=True, schema=schemas.category_schema)

join1_df = film_actor_df.join(actor_df, actor_df.actor_id == film_actor_df.actor_id, 'left')
join2_df = join1_df.join(film_category_df, film_actor_df.film_id == film_category_df.film_id, 'inner')
join3_df = join2_df.join(category_df, category_df.category_id == film_category_df.category_id, 'inner')

grouped_df = join3_df.groupBy(film_actor_df.actor_id, actor_df.first_name, actor_df.last_name, category_df.name).agg(
    F.sum(film_actor_df.actor_id).alias('appearance_count'))

filtered_df = grouped_df.filter(category_df.name == 'Children')

sorted_df = filtered_df.orderBy(F.desc('appearance_count'))
sorted_df = sorted_df.select('first_name', 'last_name', 'appearance_count')

limited_df = sorted_df.limit(30)
limited_df.show()
