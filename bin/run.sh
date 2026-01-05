#!/bin/bash

# ================= 配置区 =================
# 默认跑今天 (因为是测试环境)，生产环境通常是 `date -d "-1 day" +%F`
TARGET_DATE="${1:-$(date +%F)}"
HDFS_PATH="/origin_data/risk/dt=$TARGET_DATE"
# =========================================

echo "=================================================="
echo "   🚀 全链路任务启动"
echo "   📅 业务日期: $TARGET_DATE"
echo "   📂 HDFS路径: $HDFS_PATH"
echo "=================================================="

# --- Phase 0: 启动采集通道 ---
echo ">>> [Phase 0] 检查并启动采集通道..."
mkdir -p /root/risk_project/logs

# 1. 启动造数脚本
if pgrep -f mock_risk_data.py > /dev/null; then
    echo "    -> 造数脚本已在运行，跳过。"
else
    echo "    -> 启动 Python 造数脚本..."
    cd /root/risk_project/code
    # 强制传入日期，保证生成的数据和处理的日期一致
    nohup python3 mock_risk_data.py --date "$TARGET_DATE" --interval 0.1 --count 2000 > /dev/null 2>&1 &
fi

# 2. 启动 Flume
if jps | grep Application > /dev/null; then
    echo "    -> Flume 已在运行，跳过。"
else
    echo "    -> 启动 Flume 采集与落地..."
    nohup /opt/module/flume/bin/flume-ng agent -n a1 -c conf -f /opt/module/flume/conf/file_to_kafka.conf -Dflume.root.logger=INFO,console > /root/risk_project/logs/flume_a1.log 2>&1 &
    nohup /opt/module/flume/bin/flume-ng agent -n a2 -c conf -f /opt/module/flume/conf/kafka_to_hdfs.conf -Dflume.root.logger=INFO,console > /root/risk_project/logs/flume_a2.log 2>&1 &
    
    echo "    -> Flume 已启动，等待 60秒 让数据落盘..."
    # 倒计时效果
    for i in {60..1}; do echo -ne "    ⏳ 剩余 $i 秒...\r"; sleep 1; done
    echo ""
fi

# 3. 关键检查：HDFS 是否真的有数据？
echo "    -> 检查 HDFS 数据..."
hadoop fs -test -e $HDFS_PATH
if [ $? -ne 0 ]; then
    echo "❌ [ERROR] HDFS 路径不存在: $HDFS_PATH"
    echo "    可能是 Flume 没配置好 dt= 前缀，或者数据没生成。"
    echo "    请检查 flume 日志。"
    exit 1
fi
echo "    -> ✅ HDFS 数据就绪！"


# --- Phase 1: 挂载 ODS ---
echo ">>> [Phase 1] 挂载 ODS 分区..."
/opt/module/spark/bin/spark-sql -e "ALTER TABLE risk_db.ods_risk_log_inc ADD IF NOT EXISTS PARTITION (dt='$TARGET_DATE') LOCATION '$HDFS_PATH';"

# --- Phase 2: DWD 清洗 ---
echo ">>> [Phase 2] 提交 DWD 清洗..."
/opt/module/spark/bin/spark-submit \
  --master yarn --deploy-mode client \
  --driver-memory 1g --executor-memory 1g --executor-cores 1 \
  /root/risk_project/code/dwd_risk_log.py "$TARGET_DATE"
if [ $? -ne 0 ]; then echo "❌ DWD 失败"; exit 1; fi

# --- Phase 3: DWS 聚合 ---
echo ">>> [Phase 3] 提交 DWS 聚合..."
/opt/module/spark/bin/spark-submit \
  --master yarn --deploy-mode client \
  --driver-memory 1g --executor-memory 1g --executor-cores 1 \
  /root/risk_project/code/dws_user_profile.py "$TARGET_DATE"
if [ $? -ne 0 ]; then echo "❌ DWS 失败"; exit 1; fi

# --- Phase 4: ADS 风控 ---
echo ">>> [Phase 4] 提交 ADS 风控报表..."
/opt/module/spark/bin/spark-submit \
  --master yarn --deploy-mode client \
  --driver-memory 1g --executor-memory 1g --executor-cores 1 \
  /root/risk_project/code/ads_risk_alert.py "$TARGET_DATE"
if [ $? -ne 0 ]; then echo "❌ ADS 失败"; exit 1; fi

# --- Phase 5: 导出 MySQL ---
echo ">>> [Phase 5] 导出黑名单到 MySQL..."
/opt/module/spark/bin/spark-submit \
  --master yarn --deploy-mode client \
  --driver-memory 1g --executor-memory 1g --executor-cores 1 \
  /root/risk_project/code/export_to_mysql.py "$TARGET_DATE"
if [ $? -ne 0 ]; then echo "❌ 导出失败"; exit 1; fi

echo "============================================================="
echo "✅✅✅ 全链路计算完成！请去 DataGrip 查看 ads_black_list 表！"
echo "============================================================="
