# 电商用户行为风控系统 (E-commerce Risk Control System)

![Lambda Architecture](https://img.shields.io/badge/Architecture-Lambda-blue)
![Spark](https://img.shields.io/badge/Spark-3.x-orange)
![Flink](https://img.shields.io/badge/Hadoop-3.x-green)

##  项目简介
本项目是一个基于 **Lambda 架构** 的大数据风控系统，旨在解决电商场景下的**恶意刷单**、**薅羊毛**等安全问题。系统实现了从日志采集、实时/离线计算到数据可视化的全链路闭环。

##  技术架构
项目采用 **离线(Batch) + 实时(Speed)** 双链路架构：

*   **数据源**: Python 脚本模拟实时埋点日志 (Mock Data)。
*   **数据接入**: Flume + Kafka (削峰填谷)。
*   **离线链路 (T+1)**:
    *   **存储**: HDFS + Hive (ODS/DWD/DWS/ADS 分层设计)。
    *   **计算**: SparkSQL (Parquet 列式存储 + 分区剪裁优化)。
    *   **产出**: 历史黑名单、用户行为画像。
*   **实时链路 (Real-time)**:
    *   **计算**: Spark Structured Streaming (微批处理)。
    *   **存储**: Redis (实时指标)。
*   **数据应用**:
    *   **展示**: Streamlit 可视化大屏 (集成 MySQL + Redis 数据)。
    *   **调度**: Shell 脚本自动化调度。

## 📂 目录结构
```text
risk_project/
├── bin/          # 自动化运维与调度脚本 (super_run.sh)
├── code/         # PySpark 清洗逻辑、实时计算、造数脚本
├── conf/         # Flume 采集与落地配置文件
├── sql/          # Hive/MySQL 建表语句
└── README.md     # 项目说明文档
```

##  核心功能
1.  **全自动部署**: 提供 `reset.sh`，1分钟内完成 Hadoop/ZK/Kafka/Hive 集群的格式化与启动。
2.  **一键启动**: `run.sh` 脚本实现了从造数据、采集到报表生成的全流程自动化。
3.  **双模风控**:
    *   **实时**: 监控全站流量与特定事件（登录/支付）QPS。
    *   **离线**: 识别"1秒内点击5次"的脚本怪，以及异地登录异常。

##  快速开始

### 1. 环境准备
*   阿里云 ECS (CentOS 7.9, 4C8G) x 3
*   Python 3.6+
*   Java 1.8

### 2. 启动集群
```bash
# 执行重置脚本 (慎用，会清空数据)
/usr/local/bin/reset.sh
```

### 3. 运行项目
```bash
# 启动全链路
/bin/run.sh
```

### 4. 查看大屏
访问 `http://<Master_IP>:8501` 查看实时风控大屏。

##  作者
*   **Developer**: 崔浩然
*   **Status**: V1.0 完成 (正在制作 Flink V2.0)