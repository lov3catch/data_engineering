from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DataFrameAPIExample") \
    .getOrCreate()

rental_schema = StructType([
    StructField('rental_id', IntegerType(), False),
    StructField('rental_date', TimestampType(), False),
    StructField('inventory_id', IntegerType(), False),
    StructField('customer_id', IntegerType(), False),
    StructField('return_date', TimestampType(), False),
    StructField('staff_id', IntegerType(), False),
    StructField('last_update', TimestampType(), False),
])

payment_schema = StructType([
    StructField('payment_id', IntegerType(), False),
    StructField('customer_id', IntegerType(), False),
    StructField('staff_id', IntegerType(), False),
    StructField('rental_id', IntegerType(), False),
    StructField('amount', DoubleType(), False),
    StructField('payment_date', TimestampType(), False),
])

inventory_schema = StructType([
    StructField('inventory_id', IntegerType(), False),
    StructField('film_id', IntegerType(), False),
    StructField('store_id', IntegerType(), False),
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

rental_df = spark.read.csv('./data/rental.csv', header=True, schema=rental_schema)
payment_df = spark.read.csv('./data/payment.csv', header=True, schema=payment_schema)
inventory_df = spark.read.csv('./data/inventory.csv', header=True, schema=inventory_schema)
film_category_df = spark.read.csv('./data/film_category.csv', header=True, schema=film_category_schema)
category_df = spark.read.csv('./data/category.csv', header=True, schema=category_schema)

join1_df = rental_df.join(payment_df, rental_df.rental_id == payment_df.rental_id, 'left')
join2_df = join1_df.join(inventory_df, rental_df.inventory_id == inventory_df.inventory_id, 'left')
join3_df = join2_df.join(film_category_df, inventory_df.film_id == film_category_df.film_id, 'left')
final_df = join3_df.join(category_df, film_category_df.category_id == category_df.category_id, 'left')

grouped_df = final_df.groupBy(category_df.name).agg(F.sum(payment_df.amount).alias('total_amount'))

sorted_df = grouped_df.orderBy(F.desc("total_amount"))
sorted_df.select('total_amount', 'name').orderBy(F.desc('total_amount')).show()
