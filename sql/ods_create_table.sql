-- 1. 建库 (标准起手式：防止报错，且指定位置)
CREATE DATABASE IF NOT EXISTS risk_db
COMMENT '风控项目数仓'
LOCATION '/warehouse/risk_db'; -- 可选，指定数仓根目录，不写就用默认

USE risk_db;

-- 2. 删旧表 (开发环境常用，生产环境慎用)
DROP TABLE IF EXISTS ods_risk_log_inc;

-- 3. 建表核心逻辑
CREATE EXTERNAL TABLE ods_risk_log_inc (
    `user_id` STRING COMMENT '用户ID',
    `event_time` STRING COMMENT '事件时间',
    `event_type` STRING COMMENT '事件类型',
    `ip_address` STRING COMMENT 'IP地址',
    `device_id` STRING COMMENT '设备ID',
    `item_id` STRING COMMENT '商品ID'
)
COMMENT 'ODS层-用户行为原始日志表(增量)'
PARTITIONED BY (`dt` STRING COMMENT '日期分区')
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' -- 行业标准JSON解析器
STORED AS TEXTFILE
LOCATION '/origin_data/risk';