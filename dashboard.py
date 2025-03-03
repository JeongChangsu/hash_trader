# dashboard.py
import streamlit as st
import pandas as pd
import psycopg2
import redis
import json
import plotly.graph_objects as go
from datetime import datetime, timedelta
from config.settings import POSTGRES_CONFIG, REDIS_CONFIG

# 페이지 설정
st.set_page_config(
    page_title="HashTrader 데이터 모니터링",
    page_icon="📊",
    layout="wide"
)


# 데이터베이스 연결
@st.cache_resource
def get_db_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


@st.cache_resource
def get_redis_connection():
    return redis.Redis(**REDIS_CONFIG)


conn = get_db_connection()
r = get_redis_connection()

# 사이드바 메뉴
selected_data = st.sidebar.selectbox(
    "데이터 선택",
    ["OHLCV 데이터", "거래량 프로필", "공포&탐욕 지수", "청산 히트맵", "온체인 데이터"]
)

# 기타 필터 (심볼, 시간프레임 등)
symbol = st.sidebar.selectbox("심볼", ["BTC/USDT", "ETH/USDT"])
timeframe = st.sidebar.selectbox("시간프레임", ["1m", "5m", "15m", "1h", "4h", "1d", "1w"])

# 시간 범위
time_range = st.sidebar.selectbox(
    "시간 범위",
    ["최근 1시간", "최근 24시간", "최근 7일", "최근 30일"]
)

# 여기서 선택한 데이터 유형에 따라 다른 내용 표시
if selected_data == "OHLCV 데이터":
    st.header(f"{symbol} {timeframe} OHLCV 데이터")

    # PostgreSQL에서 데이터 가져오기
    cursor = conn.cursor()
    cursor.execute("""
        SELECT timestamp_ms, open, high, low, close, volume
        FROM market_ohlcv
        WHERE symbol = %s AND timeframe = %s
        ORDER BY timestamp_ms DESC
        LIMIT 100
    """, (symbol, timeframe))

    data = cursor.fetchall()
    cursor.close()

    if data:
        # 데이터프레임 생성
        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.sort_values('timestamp')

        # 캔들스틱 차트
        fig = go.Figure(data=[go.Candlestick(
            x=df['timestamp'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name='캔들스틱'
        )])

        fig.update_layout(
            title=f'{symbol} {timeframe} 차트',
            xaxis_title='시간',
            yaxis_title='가격',
            height=600,
            template='plotly_dark'
        )

        st.plotly_chart(fig, use_container_width=True)

        # 거래량 차트
        volume_fig = go.Figure(data=[go.Bar(
            x=df['timestamp'],
            y=df['volume'],
            name='거래량',
            marker_color='rgba(46, 204, 113, 0.7)'
        )])

        volume_fig.update_layout(
            title=f'{symbol} {timeframe} 거래량',
            xaxis_title='시간',
            yaxis_title='거래량',
            height=300,
            template='plotly_dark'
        )

        st.plotly_chart(volume_fig, use_container_width=True)

        # 생 데이터 테이블
        st.subheader("최근 데이터")
        st.dataframe(df.sort_values('timestamp', ascending=False).head(10))
    else:
        st.error("데이터가 없습니다")

# 유사한 방식으로 다른 데이터 유형에 대한 시각화 로직 추가
elif selected_data == "공포&탐욕 지수":
    st.header("비트코인 공포&탐욕 지수")

    # Redis에서 최신 데이터 가져오기
    fear_greed_data = r.get("data:fear_greed:latest")

    if fear_greed_data:
        data = json.loads(fear_greed_data)

        # 게이지 표시
        fg_index = data["fear_greed_index"]
        category = data["sentiment_category"]

        # 색상 결정
        color = "red"
        if category == "Extreme Fear":
            color = "darkred"
        elif category == "Fear":
            color = "red"
        elif category == "Neutral":
            color = "yellow"
        elif category == "Greed":
            color = "green"
        elif category == "Extreme Greed":
            color = "darkgreen"

        # 게이지 차트
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=fg_index,
            title={"text": f"공포&탐욕 지수: {category}"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": color},
                "steps": [
                    {"range": [0, 25], "color": "darkred"},
                    {"range": [25, 45], "color": "red"},
                    {"range": [45, 55], "color": "yellow"},
                    {"range": [55, 75], "color": "green"},
                    {"range": [75, 100], "color": "darkgreen"}
                ]
            }
        ))

        fig.update_layout(height=400, template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)

        # 추가 정보
        col1, col2, col3 = st.columns(3)
        col1.metric("24시간 변화", f"{data.get('change_rate', 0):.2f}%")
        col1.metric("신호", data.get("signal", "neutral"))

        # 최근 데이터 가져오기 (PostgreSQL)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT date_value, fear_greed_index, sentiment_category
            FROM fear_greed_index
            ORDER BY date_value DESC
            LIMIT 30
        """)

        history_data = cursor.fetchall()
        cursor.close()

        if history_data:
            df = pd.DataFrame(history_data, columns=['date', 'index', 'category'])

            # 선 차트
            line_fig = go.Figure(data=go.Scatter(
                x=df['date'],
                y=df['index'],
                mode='lines+markers',
                name='공포&탐욕 지수',
                line={"color": "skyblue"}
            ))

            line_fig.update_layout(
                title='30일 공포&탐욕 지수 추이',
                xaxis_title='날짜',
                yaxis_title='지수',
                height=400,
                template='plotly_dark'
            )

            st.plotly_chart(line_fig, use_container_width=True)

            st.subheader("최근 기록")
            st.dataframe(df.head(10))

# 나머지 데이터 유형에 대한 시각화 코드...

# 실행: streamlit run dashboard.py