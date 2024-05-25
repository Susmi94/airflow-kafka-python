import logging

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import time



def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        emp_id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('emp_id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(emp_id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0," 
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config("spark.cassandra.connection.host", "127.0.0.1") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
            .config("spark.sql.catalog.lakehouse", "com.datastax.spark.connector.datasource.CassandraCatalog") \
            .config("spark.local.dir", "C:/path/to/custom/temp/dir") \
            .getOrCreate()
        print(s_conn.sparkContext.appName)

        # .config('spark.cassandra.connection.host', 'localhost') \

        # s_conn.sparkContext.setLogLevel("DEBUG") ##ERROR
        # print(s_conn)
        print("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    # 'broker:29092' 'localhost:9092'
    spark_df = None
    print('Hellooooooooooooo  spark_df', spark_df);
    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092,localhost:9092") \
            .option("subscribe", "users_created") \
            .load()
            # .option('startingOffsets', 'earliest') \

        print('Hellooooooooooooo  spark_df', spark_df);
        print(f"Is streaming?: {spark_df.isStreaming}")
        spark_df.printSchema()
        # for key, value in spark_df.options.items():
        #     print(f"{key}: {value}")

        logging.info("kafka dataframe created successfully")

        # parsed_df = spark_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        #
        # # Debugging: Print the parsed_df to console to verify data
        # console_query = parsed_df.writeStream \
        #     .outputMode('append') \
        #     .format('console') \
        #     .start()
        #
        # # Check if the query is active and if there are records processed
        # if query.isActive:
        #     print("Streaming query is active")
        #     progress = query.lastProgress
        #     if progress:
        #         print(f"Number of input rows: {progress['numInputRows']}")
        #     else:
        #         print("No progress information available yet")
        # else:
        #     print("Streaming query is not active")

        # # Await termination to keep the streaming query running
        # console_query.awaitTermination()
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")



    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("emp_id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    # Convert value column to string and parse JSON
    sel = spark_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

    print('sel', sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session, spark_df)

            # try:
            streaming_query = (selection_df.writeStream.format('org.apache.spark.sql.cassandra')
                               .outputMode('append')  # Adjust based on your processing logic
                               .format('console')
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())
            # streaming_query = (selection_df.writeStream.format("csv")
            #                    .outputMode("append")  # Adjust based on your processing logic
            #                    .option('checkpointLocation', '/tmp/checkpoint')
            #                    .option('keyspace', 'spark_streams')
            #                    .option('table', 'created_users')
            #                    .start())
            time.sleep(10)
            # print(f"Is streaming?: {streaming_query.isStreaming}")
            print(f"Is Active?: {streaming_query.isActive}")
            print(f"status: {streaming_query.status}")
            print(f"lastProgress: {streaming_query.lastProgress}")


            streaming_query.awaitTermination()

            # except Exception as e:
            #     logging.error(f"Error during streaming query execution: {e}")

        else:
            logging.error("Cassandra session could not be established.")
    else:
        logging.error("Spark connection could not be established.")