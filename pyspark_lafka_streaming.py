from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time

kafka_topic_name = "ordersTopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
customers_data_file_path = "customers.csv"

mysql_host_name = "localhost"
mysql_port_no = "3306"
mysql_database_name = "sales_db"
mysql_driver_class = "com.mysql.cj.jdbc.Driver"
mysql_table_name = "total_sales_by_source_state"
mysql_user_name = "root"
mysql_password = "mynewpassword"
mysql_jdbc_url = "jdbc:mysql://localhost:3306/sales_db?createDatabaseIfNotExist=true"

cassandra_host_name = "localhost"
cassandra_port_no = "9042"
cassandra_kwyspace_name = "sales_ks"
cassandra_table_name = "order_tbl"


def save_to_cassandra(current_df, epoc_id):
    print("Printing before Cassandra table save11 : ", str(epoc_id))
    current_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="order_tbl", keyspace="sales_ks") \
        .save()
    print("Printing before Cassandra table save : ", str(epoc_id))


def save_to_mysql(current_df, epoc_id):
    processed_at = time.strftime("%Y-%m-%d %H:%M:%S")

    current_df_final = current_df \
        .withColumn("processed_at", lit(processed_at)) \
        .withColumn("batch_id", lit(epoc_id))
    print(current_df_final.printSchema())
    print("Printing before Msql table save: " + str(epoc_id))
    current_df_final.show()
    current_df_final \
        .write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/sales_db?characterEncoding=UTF-8") \
        .option("dbtable", "total_sales_by_source_state") \
        .option("user", "root") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("password", "mynewpassword") \
        .mode("append") \
        .save()


print("Printing after Msql table save: ")

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")
    print("Data Processing Application Started...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars",
                "con/jsr166e-1.0.0.jar,"
                "jars/spark-cassandra-connector_2.12-3.0.1.jar,"
                "jars/spark-streaming-kafka-0-10_2.12-3.2.1.jar,"
                "jars/spark-cassandra-connector-driver_2.12-3.0.0-beta-javadoc.jar,"
                "jars/mysql-connector-java-8.0.29.jar,"
                "jars/commons-pool2-2.11.1.jar,"
                "jars/spark-cassandra-connector-assembly_2.12-2.5.2.jar,"
                "jars/cassandra-driver-core-3.11.2.jar,"
                "jars/spark-token-provider-kafka-0-10_2.12-3.3.0.jar,"
                "jars/spark-sql-kafka-0-10_2.12-3.2.1.jar,"
                "jars/kafka-clients-3.2.1.jar") \
        .config("spark.executor.extraClassPath",
                "con/jsr166e-1.0.0.jar,"
                "jars/cassandra-driver-core-3.11.2.jar,"
                "jars/spark-cassandra-connector_2.12-3.0.1.jar,"
                "jars/spark-cassandra-connector-driver_2.12-3.0.0-beta-javadoc.jar,"
                "jars/spark-streaming-kafka-0-10_2.12-3.2.1.jar,"
                "jars/commons-pool2-2.11.1.jar,"
                "jars/spark-token-provider-kafka-0-10_2.12-3.3.0.jar,"
                "jars/mysql-connector-java-8.0.29.jar,"
                "jars/spark-cassandra-connector-assembly_2.12-2.5.2.jar,"
                "jars/spark-sql-kafka-0-10_2.12-3.2.1.jar,"
                "jars/kafka-clients-3.2.1.jar") \
        .config("spark.executor.extraLibrary",
                "con/jsr166e-1.0.0.jar, "
                "jars/cassandra-driver-core-3.11.2.jar,"
                "jars/spark-cassandra-connector-driver_2.12-3.0.0-beta-javadoc.jar,"
                "jars/spark-cassandra-connector_2.12-3.0.1.jar,"
                "jars/spark-streaming-kafka-0-10_2.12-3.2.1.jar,"
                "jars/commons-pool2-2.11.1.jar,"
                "jars/spark-cassandra-connector-assembly_2.12-2.5.2.jar,"
                "jars/spark-token-provider-kafka-0-10_2.12-3.3.0.jar,"
                "jars/spark-sql-kafka-0-10_2.12-3.2.1.jar,"
                "jars/mysql-connector-java-8.0.29.jar,"
                "jars/kafka-clients-3.2.1.jar") \
        .config("spark.driver.extraClassPath",
                "con/jsr166e-1.0.0.jar, "
                "jars/cassandra-driver-core-3.11.2.jar,"
                "jars/spark-cassandra-connector-driver_2.12-3.0.0-beta-javadoc.jar,"
                "jars/spark-cassandra-connector_2.12-3.0.1.jar,"
                "jars/spark-cassandra-connector-assembly_2.12-2.5.2.jar,"
                "jars/spark-streaming-kafka-0-10_2.12-3.2.1.jar,"
                "jars/commons-pool2-2.11.1.jar,"
                "jars/spark-token-provider-kafka-0-10_2.12-3.3.0.jar,"
                "jars/spark-sql-kafka-0-10_2.12-3.2.1.jar,"
                "jars/mysql-connector-java-8.0.29.jar,"
                "jars/kafka-clients-3.2.1.jar") \
        .config("spark.cassandra.connection.host", cassandra_host_name) \
        .config("spark.cassandra.connection.port", cassandra_port_no) \
        .getOrCreate()
    print("SSSSSSSSSSSSSSSSSSSSSSSSSs")
    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from testtopic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of transaction_detail_df: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the transaction_detail data
    orders_schema = StructType() \
        .add("order_id", StringType()) \
        .add("created_at", StringType()) \
        .add("discount", StringType()) \
        .add("product_id", StringType()) \
        .add("quantity", StringType()) \
        .add("subtotal", StringType()) \
        .add("tax", StringType()) \
        .add("total", StringType()) \
        .add("customer_id", StringType())

    orders_df2 = orders_df1 \
        .select(from_json(col("value"), orders_schema).alias("orders"), "timestamp")
    print("SSSSSSSSSSSSSSSSSSSSSSSSSs 1")

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    orders_df3.printSchema()
    print("SSSSSSSSSSSSSSSSSSSSSSSSSs 2")
    orders_df3 \
        .writeStream \
        .trigger(processingTime='15 seconds') \
        .outputMode("update") \
        .foreachBatch(save_to_cassandra) \
        .start()
    # print(orders_df3.printSchema(), " SSSSSSSSSSSSSSSSSSSSSSSSSs3")
    customers_df = spark.read.csv(customers_data_file_path, header=True, inferSchema=True)
    customers_df.printSchema()
    customers_df.show(5, False)
    print("SSSSSSSSSSSSSSSSSSSSSSSSSs4")

    print(orders_df3)
    print("SSSSSSSSSAAAAAAAAAAAAAAFFFFFFFFFFFFFFFFFFFFF")
    print(customers_df)
    orders_df4 = orders_df3.join(customers_df, orders_df3.customer_id == customers_df.customer_id, how='inner')
    print("Printing Schema of orders_df4 ")
    orders_df4.printSchema()
    # orders_df4.writeStream.format("console").outputMode("append").start().awaitTermination()
    # orders_df4.awaitTermination()
    orders_df5 = orders_df4.groupBy("source", "state") \
        .agg({'total': 'sum'}) \
        .select("source", "state", col("sum(total)").alias("total_sum_amount"))

    print("Printing Schema of orders_df5: ")
    orders_df5.printSchema()
    # orders_df5.writeStream.format("console").outputMode("append").start().awaitTermination()

# Write final result into console for debugging purpose

trans_detail_write_stream = orders_df5 \
    .writeStream \
    .trigger(processingTime='15 seconds') \
    .outputMode("update") \
    .option("truncate", "false") \
    .format("console") \
    .start()

orders_df5 \
    .writeStream \
    .trigger(processingTime='15 seconds') \
    .outputMode("update") \
    .foreachBatch(save_to_mysql) \
    .start()

trans_detail_write_stream.awaitTermination()

print("PySpark Structured Streaming with Kafka Demo Application Completed.")
