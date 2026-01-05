import streamlit as st
import pandas as pd
import pymysql
import redis
import time
from datetime import datetime

# --- 1. 页面基础设置 ---
st.set_page_config(
    page_title="风控大脑监控",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- 2. 注入 CSS 美化 ---
st.markdown("""
<style>
    .main {
        background-color: #0E1117;
    }
    div[data-testid="metric-container"] {
        background-color: #262730;
        border: 1px solid #464B5C;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.5);
        color: #FFFFFF;
    }
    /* 1.10.0 版本的表格样式兼容 */
    .stDataFrame {
        background-color: #262730;
    }
    h1, h2, h3 {
        color: #FAFAFA !important;
        font-family: 'Helvetica Neue', sans-serif;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


# --- 3. 连接资源 (兼容旧版本写法) ---

# 【关键修改 1】把 st.cache_resource 改为 st.experimental_singleton
# 这是 1.10.0 版本中用于缓存数据库连接的专用装饰器
@st.experimental_singleton
def init_redis():
    try:
        # 使用连接池防止频繁连接报错
        pool = redis.ConnectionPool(host='master', port=6379, decode_responses=True)
        return redis.Redis(connection_pool=pool)
    except:
        return None


r = init_redis()


def get_mysql_data():
    try:
        conn = pymysql.connect(host='master', user='root', password='123456', database='risk_data_view',
                               charset='utf8mb4')
        sql = "SELECT user_id, risk_type, check_time, dt FROM ads_black_list ORDER BY check_time DESC LIMIT 50"
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except:
        return pd.DataFrame()


# --- 4. 页面布局 ---

st.title("🛡️ 2026 电商风控全链路监控中心")
# 【关键修改 2】st.divider() 是新功能，旧版本用 markdown 模拟
st.markdown("---")

dashboard = st.empty()

# --- 5. 实时刷新循环 ---
while True:
    try:
        # 1. 读 Redis
        try:
            cnt_login = int(r.get("realtime:count:login") or 0)
            cnt_pay = int(r.get("realtime:count:pay") or 0)
            cnt_view = int(r.get("realtime:count:view_product") or 0)
            cnt_coupon = int(r.get("realtime:count:get_coupon") or 0)
            total_traffic = cnt_login + cnt_pay + cnt_view + cnt_coupon
        except:
            cnt_login, cnt_pay, cnt_view, cnt_coupon, total_traffic = 0, 0, 0, 0, 0

        # 2. 读 MySQL
        df_black = get_mysql_data()
        black_count = len(df_black)
        last_hacker_time = df_black['check_time'].max() if not df_black.empty else "--:--:--"

        # 3. 渲染 UI
        with dashboard.container():
            st.subheader(f"⚡ 实时流量监控 (每 2 秒刷新) - {datetime.now().strftime('%H:%M:%S')}")

            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("🌊 实时总吞吐量", f"{total_traffic}", delta="Kafka源")
            k2.metric("👀 浏览行为", f"{cnt_view}", delta_color="off")
            k3.metric("🛒 登录行为", f"{cnt_login}", delta_color="off")
            k4.metric("🎟️ 领券行为", f"{cnt_coupon}", delta="High Risk")
            k5.metric("💰 支付行为", f"{cnt_pay}", delta_color="inverse")

            st.markdown("---")

            st.subheader("🛑 T+1 黑名单拦截公示")

            c1, c2 = st.columns([1, 3])

            with c1:
                st.info("数据来源: Hive -> Spark -> MySQL")
                st.metric("💀 累计拦截黑产", f"{black_count} 人", delta="昨日新增")
                st.metric("🕒 最新入库时间",
                          str(last_hacker_time)[11:] if len(str(last_hacker_time)) > 11 else str(last_hacker_time))

                if not df_black.empty:
                    st.write("风险类型分布:")
                    risk_counts = df_black['risk_type'].value_counts()
                    st.bar_chart(risk_counts)

            with c2:
                if not df_black.empty:
                    # 【关键修改 3】去掉 use_container_width 参数，旧版本不支持
                    st.dataframe(df_black, height=400)
                else:
                    st.warning("暂无黑名单数据，请检查离线任务是否完成。")

        time.sleep(2)

    except KeyboardInterrupt:
        break
    except Exception as e:
        # 防止页面报错崩溃，打印错误但不退出
        st.error(f"大屏发生临时错误: {e}")
        time.sleep(5)