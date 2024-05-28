from pyspark.sql import SparkSession, functions as F

import schemas

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DataFrameAPIExample") \
    .getOrCreate()

rental_df = spark.read.csv('./data/rental.csv', header=True, schema=schemas.rental_schema)
payment_df = spark.read.csv('./data/payment.csv', header=True, schema=schemas.payment_schema)
inventory_df = spark.read.csv('./data/inventory.csv', header=True, schema=schemas.inventory_schema)
film_category_df = spark.read.csv('./data/film_category.csv', header=True, schema=schemas.film_category_schema)
category_df = spark.read.csv('./data/category.csv', header=True, schema=schemas.category_schema)

join1_df = rental_df.join(payment_df, rental_df.rental_id == payment_df.rental_id, 'left')
join2_df = join1_df.join(inventory_df, rental_df.inventory_id == inventory_df.inventory_id, 'left')
join3_df = join2_df.join(film_category_df, inventory_df.film_id == film_category_df.film_id, 'left')
final_df = join3_df.join(category_df, film_category_df.category_id == category_df.category_id, 'left')

grouped_df = final_df.groupBy(category_df.name).agg(F.sum(payment_df.amount).alias('total_amount'))

sorted_df = grouped_df.orderBy(F.desc("total_amount"))
sorted_df.select('total_amount', 'name').orderBy(F.desc('total_amount')).show()
