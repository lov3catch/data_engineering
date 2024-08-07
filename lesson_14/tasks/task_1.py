from pyspark.sql import SparkSession, functions as F

import schemas

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DataFrameAPIExample") \
    .getOrCreate()

category_df = spark.read.csv('./data/category.csv', header=True, schema=schemas.category_schema)
film_category_df = spark.read.csv('./data/film_category.csv', header=True, schema=schemas.film_category_schema)

inner_join_df = category_df.join(film_category_df, category_df.category_id == film_category_df.category_id, 'inner')
inner_join_df.show()

grouped_df = inner_join_df.groupBy(film_category_df.category_id, category_df.name).count()
grouped_df.show()

sorted_df = grouped_df.sort(F.desc("count"))
sorted_df.select('name', 'count').show()
