from pyspark.sql import SparkSession
from pysqark.sql.functions import from_json, col, month, hour, dayofmonth, year, ufd


@ufd
def string_decode(s, encoding="utf-8"):
    if s:
        return (
            s.encode("latin1")
            .decode("unicode-escape")
            .encode("latin1")
            .decode(encoding)
            .strip('\"')
        )
    else:
        return s

def create_or_get_spark_session(app_name, master="yarn"):
    """
    Creates or gets a Spark Session

    Parameters:
        app_name: str
            Pass the name of your app
        master: str
            Choosing the Spark master, yarn is the default
    
    Returns:
        spark: SparkSession
    """
    spark= (SparkSession
        .builder
        .appName(app_name)
        .master(master=master)
        .getOrCreate()
    )
    return spark

def create_kafka_read_stream(spark, kafka_address, kafka_port, topic, starting_offset="earliest"):
    """
    Create a kafka read stream

    Parameters:
        spark: SparkSession
            A SparkSession object
        kafka_address: str
            Host address of the kafka bootstrap server
        kafka_port: int
            Port of the kafka bootstrap server
        topic: str
            Name of the kafka topic
        starting_offset: str
            Starting offset configuration, "earliest" by default
    
    Returns:
        read_stream: DataStreamReader
    """

    read_stream= (spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",f"{kafka_address}:{kafka_port}")
        .option("failOnDataLoss", False)
        .option("startingOffsets", starting_offset)
        .option("subscribe", topic)
        .load()
    )

    return read_stream

def process_stream(stream, stream_schema, topic):
    """
    Process stream to fetch on value from the kafka message.
    Convert ts to timpstamp format and produce year, month, day, hours columns

    Parameters:
        stream: DataStreamReader
            The data stream reader for your stream
        stream_schema: StructType
            The schema for your stream
        topic: str
            Name of the kafka topic
    
    Returns:
        processed_stream: DataFrame
    """
    # Read onlu value from the incoming message and convert the contents
    # Inside to the pass schema
    stream= (stream
        .selectExpr("CAST(value as String)")
        .select(
            from_json(col("value"), schema=stream_schema).alias("data")
        )
        .select(
            "data.*"
        )
    )

    # Add month, day, hour to split the data into separate directories
    stream= (stream
        .withColumn("ts", (col("ts")/1000).cast("timestamp"))
        .withColumn("year",year(col("ts")))
        .withColumn("month",month(col("ts")))
        .withColumn("day",day(col("ts")))
        .withColumn("hour",hour(col("ts")))
    )

    if topic in ["listen_events", "page_view_events"]:
        stream= (stream
            .withColumn("song", string_decode("song"))
            .withColumn("artist", string_decode("artist"))
        )
    
    return stream


def create_file_write_stream(stream, storage_path, checkpoint_path, trigger='120 seconds', output_mode="append", file_format="parquet"):
    """
    Write the stream back to to file store

    Parameters:
        stream: DataStreamReader
            The data stream reader for your stream
        file_format: str
            parquet, csv, orc etc
        storage_path: str
            The file output path
        checkpoint_path: str
            The checkpoint location for spark
        trigger: str
            The trigger interval
        output_mode: str
            append, complete, update
    """
    write_stream= (stream
        .writeStream
        .format(file_format)
        .partionBy("month","day","hour")
        .option("path",storage_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger)
        .outputMode(output_mode)
    )

    return write_stream