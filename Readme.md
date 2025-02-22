docker exec -it  postgres psql -U airflow -d airflow


docker exec -it scheduler /bin/bash -c "pip install kafka-python"
docker exec -it webserver /bin/bash -c "pip install kafka-python"


docker cp -L D:\BinK\projectdata\pyspark_scripts\test_pyspark.py spark-master:/opt/bitnami/spark/test_pyspark.py

docker logs spark-master


docker exec -it spark-master cat /etc/hosts
echo "172.18.0.8 namenode" >> /etc/hosts


wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz
tar -xzf hadoop-3.2.1.tar.gz -C /opt/
export PATH=$PATH:/opt/hadoop-3.2.1/bin

docker exec -it spark-master spark-submit --master spark://172.18.0.16:7077 --deploy-mode client --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 test_pyspark.py