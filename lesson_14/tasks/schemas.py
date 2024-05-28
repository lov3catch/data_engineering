from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, DoubleType

# category_schema = StructType([
#     StructField('category_id', IntegerType(), False),
#     StructField('name', StringType(), False),
#     StructField('last_update', DateType(), False),
# ])

# film_category_schema = StructType(
#     [
#         StructField('film_id', IntegerType(), False),
#         StructField('category_id', IntegerType(), False),
#         StructField('last_update', DateType(), False),
#     ]
# )

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

# inventory_schema = StructType([
#     StructField('inventory_id', IntegerType(), False),
#     StructField('film_id', IntegerType(), False),
#     StructField('store_id', IntegerType(), False),
#     StructField('last_update', TimestampType(), False),
# ])

actor_schema = StructType([
    StructField('actor_id', IntegerType(), False),
    StructField('first_name', StringType(), False),
    StructField('last_name', StringType(), False),
    StructField('last_update', TimestampType(), False),
])

# rental_schema = StructType([
#     StructField('rental_id', IntegerType(), False),
#     StructField('rental_date', TimestampType(), False),
#     StructField('inventory_id', IntegerType(), False),
#     StructField('customer_id', IntegerType(), False),
#     StructField('return_date', TimestampType(), False),
#     StructField('staff_id', IntegerType(), False),
#     StructField('last_update', TimestampType(), False),
# ])

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

# inventory_schema = StructType([
#     StructField('inventory_id', IntegerType(), False),
#     StructField('film_id', IntegerType(), False),
#     StructField('store_id', IntegerType(), False),
#     StructField('last_update', DateType(), False),
# ])

film_schema = StructType(
    [
        StructField('film_id', IntegerType(), False),
        StructField('title', StringType(), False),
        StructField('description', StringType(), False),
        StructField('release_year', IntegerType(), False),
        StructField('language_id', IntegerType(), False),
        StructField('original_language_id', IntegerType(), True),
        StructField('rental_duration', IntegerType(), False),
        StructField('rental_rate', StringType(), False),
        StructField('length', IntegerType(), False),
        StructField('replacement_cost', IntegerType(), False),
        StructField('rating', IntegerType(), False),
        StructField('special_features', StringType(), False),
        StructField('fulltext', StringType(), False),
        StructField('last_update', DateType(), False),
    ]
)

# film_actor_schema = StructType([
#     StructField('actor_id', IntegerType(), False),
#     StructField('film_id', IntegerType(), False),
#     StructField('last_update', TimestampType(), False),
# ])

# actor_schema = StructType([
#     StructField('actor_id', IntegerType(), False),
#     StructField('first_name', StringType(), False),
#     StructField('last_name', StringType(), False),
#     StructField('last_update', TimestampType(), False),
# ])

# film_category_schema = StructType([
#     StructField('film_id', IntegerType(), False),
#     StructField('category_id', IntegerType(), False),
#     StructField('last_update', TimestampType(), False),
# ])

# category_schema = StructType([
#     StructField('category_id', IntegerType(), False),
#     StructField('name', StringType(), False),
#     StructField('last_update', TimestampType(), False),
# ])
