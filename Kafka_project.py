import os

from datetime import current_timestamp, unix_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType
import psycopg2


TOPIC_NAME_OUT = 'student.topic.cohort2.ksn098.out'
TOPIC_NAME_IN = 'student.topic.cohort2.ksn098'

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
}


# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()

    # создаем таблицу в PostgreSQL, записывая данные в неё
    connection = psycopg2.connect(f"host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'")
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.subscribers_feedback (
        id serial4 NOT NULL,
        restaurant_id text NOT NULL,
        adv_campaign_id text NOT NULL,
        adv_campaign_content text NOT NULL,
        adv_campaign_owner text NOT NULL,
        adv_campaign_owner_contact text NOT NULL,
        adv_campaign_datetime_start int8 NOT NULL,
        adv_campaign_datetime_end int8 NOT NULL,
        datetime_created int8 NOT NULL,
        client_id text NOT NULL,
        trigger_datetime_created int4 NOT NULL,
        feedback varchar NULL,
        CONSTRAINT id_pk PRIMARY KEY (id)
        ); """)
    connection.commit()

    # записываем df в PostgreSQL с полем feedback
    df.withColumn("feedback", lit(None).cast(StringType())) \
        .write.format("jdbc") \
        .mode('append') \
        .option('url', 'jdbc:postgresql://localhost:5432/de') \
        .option('dbtable', 'public.subscribers_feedback') \
        .option('user', 'jovyan') \
        .option('password', 'jovyan') \
        .option('driver', 'org.postgresql.Driver') \
        .save()
    cursor.close()
    connection.close()

    # создаём df для отправки в Kafka. Сериализация в json.
    df2 = (df.withColumn('value', to_json(struct( col('restaurant_id'), col('adv_campaign_id'),\
                col('adv_campaign_content'), col('adv_campaign_owner'),\
                col('adv_campaign_owner_contact'), col('adv_campaign_datetime_start'),\
                col('adv_campaign_datetime_end'), col('datetime_created'),\
                col('client_id'), col('trigger_datetime_created')))
            )).select('value')
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df2.writeStream \
        .format("kafka") \
        .outputMode("append")\
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .options(**kafka_security_options) \
        .option('topic', TOPIC_NAME_OUT) \
        .option("checkpointLocation", "query")\
        .option("truncate", False)\
        .save()
    # очищаем память от df
    df.unpersist()


# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(["org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0", "org.postgresql:postgresql:42.4.0", ] )

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов
restaurant_read_stream_df = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";') \
        .option("subscribe", "student.topic.cohort2.ksn098") \
        .load()

# определяем схему входного сообщения для json
incomming_message_schema = StructType([StructField("restaurant_id", StringType()), StructField("adv_campaign_id", StringType()),\
            StructField("adv_campaign_content", StringType()), StructField("adv_campaign_owner", StringType()),\
            StructField("adv_campaign_owner_contact", StringType()),  StructField("adv_campaign_datetime_start", LongType()),\
            StructField("adv_campaign_datetime_end", LongType()), StructField("datetime_created", LongType()) ])

# определяем текущее время в UTC в миллисекундах, затем округляем до секунд
current_timestamp_utc = int(round(unix_timestamp(current_timestamp())))

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = restaurant_read_stream_df \
        .withColumn('value', restaurant_read_stream_df.value.cast(StringType()))\
        .withColumn('deserialized_value', from_json(col("value"), schema=incomming_message_schema))\
        .select("deserialized_value.*")\
        .filter((col("adv_campaign_datetime_start") <= current_timestamp_utc) & (col("adv_campaign_datetime_end") >= current_timestamp_utc))

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'student') \
                    .option('password', 'de-student') \
                    .load()

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_read_stream_df.join(subscribers_restaurant_df, 'restaurant_id')\
    .withColumn("trigger_datetime_created", lit(current_timestamp_utc)).drop('id')\
    .dropDuplicates(['client_id', 'restaurant_id'])
# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()
