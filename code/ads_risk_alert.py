# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# --- 获取外部传入的日期参数 ---
if len(sys.argv) > 1:
    target_date = sys.argv[1]  # 获取第一个参数
else:
    print('未传入日期参数')

spark = SparkSession.builder \
    .appName(f"ADS_Risk_Alert{target_date}") \
    .enableHiveSupport() \
    .getOrCreate()

print(f">>> 🚀 开始执行 ADS 风控规则扫描 ({target_date})...")

# --- 核心风控 SQL ---
# 规则：同一用户，连续 5 次操作的时间差 < 1 秒
sql = f"""
SELECT DISTINCT user_id, '高频刷单' as risk_type, current_timestamp() as check_time
FROM (
    SELECT 
        user_id,
        event_time,
        LEAD(event_time, 4) OVER (PARTITION BY user_id ORDER BY event_time) as next_5th_time
    FROM risk_db.dwd_risk_log_inc
    WHERE dt = '{target_date}'
) t
-- 只要时间差 <= 1秒 (含0秒) 都算
WHERE (unix_timestamp(next_5th_time) - unix_timestamp(event_time)) <= 1
"""

df_black_list = spark.sql(sql)

print(">>> 😱 发现疑似黑产用户：")
df_black_list.show()

# 增加分区列 dt
df_final = df_black_list.withColumn("dt", lit(target_date))

print(">>> 💾 存入 ADS 表 (Parquet)...")
# 关键：模式设为 overwrite，格式 parquet，指定分区
df_final.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("dt") \
    .option("path", f"hdfs://master:8020/warehouse/risk/ads/ads_black_list/dt={target_date}") \
    .saveAsTable("risk_db.ads_black_list")