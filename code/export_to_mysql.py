# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession

# 1. 获取日期参数
if len(sys.argv) > 1:
    target_date = sys.argv[1]
else:
    target_date = "2026-01-04"

print(f">>> 🚀 开始导出 {target_date} 的数据到 MySQL...")

# 2. 初始化 Spark
spark = SparkSession.builder \
    .appName(f"Export_MySQL_{target_date}") \
    .enableHiveSupport() \
    .getOrCreate()

# 3. 读取 ADS 表 (Hive/HDFS)
# 注意：我们只选我们需要的列
df_ads = spark.sql(f"SELECT user_id, risk_type, cast(check_time as string) as check_time, dt FROM risk_db.ads_black_list WHERE dt='{target_date}'")

print(f"    - 待导出数据量: {df_ads.count()}")

# 4. 写入 MySQL配置
# JDBC URL (注意：useSSL=false 是必须的)
mysql_url = "jdbc:mysql://master:3306/risk_data_view?useSSL=false&useUnicode=true&characterEncoding=utf-8"

mysql_props = {
    "user": "root",
    "password": "123456",
    "driver": "com.mysql.jdbc.Driver"
}

# 5. 执行写入
# mode="append": 追加模式 (如果主键冲突会报错，生产环境通常用 replace 或先删后写，这里简化用 append)
# 为了防止主键冲突，我们先简单粗暴地用 overwrite (覆盖表) 或者 ignore (忽略冲突)
# 这里我们用 append，如果你重复跑脚本报错，请先在 MySQL 此时 truncate table
try:
    df_ads.write.jdbc(url=mysql_url, table="ads_black_list", mode="append", properties=mysql_props)
    print(">>> ✅ 导出 MySQL 成功！")
except Exception as e:
    print(f">>> ❌ 导出失败 (可能是主键冲突，请清理 MySQL 表后重试): {str(e)}")

spark.stop()
