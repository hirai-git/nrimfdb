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

def get_share_data(kabulist_date):
    querylist = f"""SELECT SECURITY_CODE, KANJI_NAME
                    FROM NPMDB.NQIUSER1.ATTRIBUTES 
                    WHERE CALENDAR_DATE = '{kabulist_date}'
                    AND SECURITY_CODE NOT LIKE '%0000%'
                    ORDER BY SECURITY_CODE;"""
    querycd=run_query(conn, querylist)
    querycd_df = pd.DataFrame(querycd, columns=['security_code', 'kanji_name'])
    querycd_df.set_index('security_code', inplace=True)
    return querycd_df


def adj_bd(date):
    """ Function for Calendar date to previous Business-day
    """
    if dt.date.weekday(date) == 5:  # if it's Saturday
        outlastbd = date - dt.timedelta(days=1)  # then make it Friday
    elif dt.date.weekday(date) == 6:  # if it's Sunday
        outlastbd = date - dt.timedelta(days=2)  # then make it Friday
    else:
        outlastbd = date
    return outlastbd
# X営業日前
def lag_bd(t0_, lag=1):
    lag_day = adj_bd(adj_bd(t0_) -
                     dt.timedelta(lag))
    return lag_day

# e.g. 2営業日前
pre_day = dt.datetime.today() - dt.timedelta(1)
previous_bizday=lag_bd(pre_day, 2)
print(previous_bizday)
# 第一営業日取得
#kabulist_date = format(jpbizday.first_bizday(dt.datetime.today()), '%Y%m%d')
kabulist_date = format(jpbizday.first_bizday(previous_bizday), '%Y%m%d')
print(kabulist_date)
kabulist =get_share_data(kabulist_date)

selector = st.sidebar.selectbox( "銘柄CD選択:",list(kabulist.index))

selector2 = selector[0:4] 
date1 = st.sidebar.date_input("from-date",
                              value=dt.date(2020, 1, 1),
                              min_value=dt.date(1992,1,1),
                              max_value=dt.date.today())
date1 = format(date1, '%Y%m%d')
date2 = st.sidebar.date_input("to-date",
                              value=dt.date.today(),
                              min_value=dt.date(1992,1,1),
                              max_value=dt.date.today())
date2 = format(date2, '%Y%m%d')

st.header("NPMdata")
meigara_to_show = kabulist.loc[selector]
st.dataframe(meigara_to_show)


def get_query( kabuid, date_from:str, date_to:str, filename:str):
    with open(filename, 'r', encoding='utf-8') as f:
        query = f.read().format(kabuid=kabuid,date_from=date_from,date_to=date_to)
    return query

st.subheader('株価グラフ表示')
#if __name__ == "__main__":
if st.button('push display'):   
    #my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
    query_file_path = 'npmdbtest.sql'
    query = get_query(kabuid=selector2,date_from=date1,date_to=date2,filename=query_file_path)
    #my_data_rows=run_query(query_file_path)
    my_data = run_query(conn, query)
    conn.close()
    print(my_data)
    my_data = pd.DataFrame(my_data, columns=['calendar_date', 'security_code', 'kanji_name', 'price']) 
    my_data.loc[:,'price']=my_data.loc[:,'price'].astype('int')
    print(my_data)
    my_chart= my_data.set_index("calendar_date")["price"]
    st.line_chart(my_chart)
    st.dataframe(my_data)

    st.subheader('20日移動平均株価')
    my_data2=my_chart
    my_data2=my_data2.rolling(20).apply(lambda x: x.mean())
    st.line_chart(my_data2)
    st.dataframe(my_data2)




    #https://www.freecodecamp.org/japanese/news/connect-python-with-sql/
    #https://linus-mk.hatenablog.com/entry/pandas_convert_float_to_int
        