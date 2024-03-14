#https://qiita.com/Ayumu-y/items/33cb9d3e9923433b734d

import streamlit as st
import pandas as pd
import requests
import snowflake.connector
import datetime as dt
import jpbizday
from sqlalchemy import create_engine,Text
import os
from dotenv import load_dotenv
from urllib.error import URLError
from snowflake.sqlalchemy import URL


st.set_page_config(
    page_title="Ex-stream-ly Cool App",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.extremelycoolapp.com/help',
        'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)

# Streamlitのシークレットを使用してSnowflakeの接続情報を取得
# C:\Users\Hirai\.streamlit\secrets.toml
snowflake_credentials = st.secrets["snowflake"]

# Snowflakeに接続
conn = snowflake.connector.connect(
    user=snowflake_credentials['user'],
    password=snowflake_credentials['password'],
    account=snowflake_credentials['account'],
    warehouse=snowflake_credentials['warehouse'],
    database=snowflake_credentials['database'],
    schema=snowflake_credentials['schema'],
    role=snowflake_credentials['role']
)

def run_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result

with st.sidebar:
    st.markdown('# 投信一覧')

if st.sidebar.button('fund data push'):
# テーブルの中身を取得して表示
    talbe_details = run_query(conn,f"select * from NRIMFDB.DBO.DTB_FUNDINFO;")
    talbe_detail_df = pd.DataFrame(talbe_details, columns=['upd','fund_cd', 'name', 'outline','stdate', 'todate', 'nikkei_url'])
    # ハイパーリンクに変換したい列を指定
    column_conf = {
    "nikkei_url": st.column_config.LinkColumn()
    }
    talbe_detail_df.set_index('fund_cd', inplace=True)
    st.markdown(f"## 投信情報一覧")
    st.dataframe(talbe_detail_df,column_config=column_conf,height=1000)
    

with st.sidebar:
    st.markdown('# テーブル選択')

    # データベースの一覧をリスト型で取得してselectboxで一つ選択
    show_databases = run_query(conn,"SHOW DATABASES;")
    database_rows = [row[1] for row in show_databases]
    select_database = st.selectbox('データベースを選択してください', database_rows)

    # スキーマの一覧をリスト型で取得してselectboxで一つ選択
    show_schemas = run_query(conn,f"SHOW SCHEMAS IN DATABASE {select_database};")
    schema_rows = [row[1] for row in show_schemas]
    select_schema = st.selectbox('スキーマを選択してください', schema_rows)

    # テーブルとビューの一覧をリスト型で取得してselectboxで一つ選択
    # SHOW TABLESだけだとviewの情報を抽出することができないので、SHOW TABLESとSHOW VIEWSを別々に実行
    show_tables = run_query(conn,f"SHOW TABLES IN {select_database}.{select_schema}")
    show_views = run_query(conn,f"SHOW VIEWS IN {select_database}.{select_schema}")
    show_dyntable = run_query(conn,f"SHOW DYNAMIC TABLES IN {select_database}.{select_schema}")
    table_rows = [row[1] for row in show_tables]
    view_rows = [row[1] for row in show_views]
    dyntable_rows = [row[1] for row in show_dyntable]
    table_view_rows = table_rows + view_rows + dyntable_rows
    select_table = st.selectbox('テーブルを選択してください', table_view_rows)

if st.sidebar.button('テーブル情報'):
    # ページのタイトル
    st.markdown(f"# データカタログ")

    # テーブルのカラムの詳細を表示
    st.markdown(f"## カラム情報：{select_table}")
    columns_details = run_query(conn,f"DESC TABLE {select_database}.{select_schema}.\"{select_table}\"")
    column_detail_df = pd.DataFrame(columns_details)
    column_detail_df = column_detail_df.rename(columns={0: 'column_name', 1: 'data_type', 9: 'comment'})
    column_detail_df = column_detail_df.loc[:, ['column_name', 'data_type', 'comment']]
    #column_widths = {'column_name': 100, 'data_type': 100, 'comment': 800}
    st.dataframe(
        column_detail_df,
        column_config={
            "column_name": st.column_config.TextColumn("カラム名", width="small"),
            "data_type": st.column_config.TextColumn("データタイプ", width="small"),
            "comment": st.column_config.TextColumn("概要", width="large",max_chars=1000,)
        },
        use_container_width=True,height=1000,
        hide_index=True
    )

