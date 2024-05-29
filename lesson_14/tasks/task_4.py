from pyspark.sql import SparkSession, functions as F

import schemas

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DataFrameAPIExample") \
    .getOrCreate()

inventory_df = spark.read.csv('./data/inventory.csv', header=True, schema=schemas.inventory_schema)
film_df = spark.read.csv('./data/film.csv', header=True, schema=schemas.film_schema)

left_join_df = film_df.join(inventory_df, inventory_df.film_id == film_df.film_id, 'left')
left_join_df.show()

filtered_df = left_join_df.filter(F.col('inventory_id').isNull())

filtered_df.select('title').distinct().show()
