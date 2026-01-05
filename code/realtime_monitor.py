# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import redis

# Redis 配置
REDIS_HOST = 'master'
REDIS_PORT = 6379

print(">>> 🚀 启动 Spark Structured Streaming 实时监控...")

spark = SparkSession.builder \
    .appName("RealTime_Risk_Monitor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. 读取 Kafka 流 (就像读一个无限增长的表)
# 注意：这里需要 Kafka 的 bootstrap servers
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "master:9092,worker1:9092,worker2:9092") \
    .option("subscribe", "risk_log_topic") \
    .option("startingOffsets", "latest") \
    .load()

# 2. 解析 JSON 数据
# Kafka 里的数据都在 'value' 列，是二进制的，先转字符串再解包
schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType())
])

df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 3. 实时聚合逻辑
# 统计每种 event_type 出现的次数
df_count = df_parsed.groupBy("event_type").count()

# 4. 写入 Redis 的函数 (微批处理)
def write_to_redis(batch_df, batch_id):
    # 这一步是在 Driver 端运行的
    print(f"--- Processing Batch {batch_id} ---")
    
    # 收集这一小批的结果到本地 (因为聚合后的数据量很小，collect没问题)
    rows = batch_df.collect()
    
    # 连接 Redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    for row in rows:
        event_type = row['event_type']
        count = row['count']
        if event_type:
            # 累加写入 Redis
            # incrby: 给 key 增加指定的值
            r.incrby(f"realtime:count:{event_type}", count)
            print(f"   -> Redis Update: {event_type} += {count}")

# 5. 启动流 (Start)
query = df_count.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_redis) \
    .start()

print(">>> ✅ 流任务已启动，正在监听 Kafka...")
query.awaitTermination()
