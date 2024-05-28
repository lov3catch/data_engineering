# Lesson_14

## How to run
```shell
docker-compose build
docker-compose up -d
```
### Using jupyter-notebook
- Open: http://127.0.0.1:8888
- Run: spark_dataframe_ht.ipynb

### Using spark-submit
```shell
docker-compose run pyspark spark-submit tasks/task_1.py
docker-compose run pyspark spark-submit tasks/task_2.py
docker-compose run pyspark spark-submit tasks/task_3.py
docker-compose run pyspark spark-submit tasks/task_4.py
docker-compose run pyspark spark-submit tasks/task_5.py
```

