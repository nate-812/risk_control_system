# -*- coding: utf-8 -*-
import sys  # <--- 1. 引入系统库
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# --- 2. 获取外部传入的日期参数 ---
if len(sys.argv) > 1:
    target_date = sys.argv[1]  # 获取第一个参数
else:
    print('未传入日期参数')

print(f">>> 🚀 开始 DWD 清洗任务，业务日期: {target_date}")

# 初始化 Spark
spark = SparkSession.builder \
    .appName(f"DWD_ETL_{target_date}") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 3. 读取 ODS (使用变量过滤) ---
# 关键点：读的时候就要过滤，只读当天的数据！
print(f">>> 读取 ODS 层数据 (dt={target_date})...")
df_ods = spark.sql(f"SELECT * FROM risk_db.ods_risk_log_inc WHERE dt='{target_date}'")
print(f"    - ODS 读取条数: {df_ods.count()}")

# 清洗逻辑
df_clean = df_ods \
    .filter("user_id IS NOT NULL") \
    .dropDuplicates(["user_id", "event_time", "item_id"])

print(f"    - 清洗后条数: {df_clean.count()}")

# --- 4. 写入 DWD (使用变量分区) ---
print(f">>> 💾 写入 DWD 层 (dt={target_date})...")

# 给数据打上当天的日期标签
df_final = df_clean.withColumn("dt", lit(target_date))

df_final.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("dt") \
    .option("path", f"hdfs://master:8020/warehouse/risk/dwd/risk_log_inc/dt={target_date}") \
    .saveAsTable("risk_db.dwd_risk_log_inc")

print(">>> ✅ DWD 层构建完成！")
spark.stop()