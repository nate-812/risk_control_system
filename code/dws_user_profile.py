# -*- coding: utf-8 -*-
import sys  # <--- 引入系统库
from pyspark.sql import SparkSession

# --- 关键修改开始 ---
# 获取命令行参数
# sys.argv[0] 是脚本文件名，sys.argv[1] 是第一个参数
if len(sys.argv) > 1:
    target_date = sys.argv[1]
else:
    # 如果没传参数，给个默认值方便测试，或者直接报错退出
    print('未传入日期参数')
# --- 关键修改结束 ---

spark = SparkSession.builder \
    .appName(f"DWS_User_Profile_{target_date}") \
    .enableHiveSupport() \
    .getOrCreate()

print(f">>> 🚀 开始构建 DWS 层画像，处理日期: {target_date} ...")

# 1. 编写 SQL 聚合逻辑
# 这里的核心是 GROUP BY user_id
sql = f"""
    SELECT 
        user_id,
        '{target_date}' as dt,
        count(*) as total_actions,
        count(distinct ip_address) as unique_ip_count,
        min(event_time) as first_active_time,
        max(event_time) as last_active_time
    FROM risk_db.dwd_risk_log_inc
    WHERE dt = '{target_date}'
    GROUP BY user_id
"""

df_dws = spark.sql(sql)

print(f"    - 聚合后的用户数: {df_dws.count()}")

# 2. 写入 DWS 表 (自动建表)
df_dws.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("dt") \
    .option("path", f"hdfs://master:8020/warehouse/risk/dws/dws_user_profile/dt={target_date}") \
    .saveAsTable("risk_db.dws_user_profile")

print(">>> ✅ DWS 层构建完成！")
spark.stop()