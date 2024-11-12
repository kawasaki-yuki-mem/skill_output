import streamlit as st
import pandas as pd
import json
from snowflake.snowpark import Session

# ファイルアップロード
file = st.file_uploader("アクセスログをアップロードしてください。")

# データを閲覧するボタン
orgdata_view = st.button('データを閲覧する')

# cnx = st.connection("snowflake")
# session = cnx.session()

with open('creds.json') as f:
    connection_parameters = json.load(f)  
session = Session.builder.configs(connection_parameters).create()

# データ閲覧ボタンを押した場合
if orgdata_view:
  df = pd.read_csv(file)
  st.write(df)

  upload_button = st.button('アップロードする')
  
  if upload_button:
    snowparkDf=session.write_pandas(df,file.name,auto_create_table = True, overwrite=True)
