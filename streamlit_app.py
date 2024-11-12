import streamlit as st
import pandas as pd
import json

# ファイルアップロード
file = st.file_uploader("アクセスログをアップロードしてください。")

# データを閲覧するボタン
orgdata_view = st.button('データを閲覧する')

cnx = st.connection("snowflake")
session = cnx.session()

# データ閲覧ボタンを押した場合
if orgdata_view:
  df = pd.read_csv(file)
  st.write(df)

  upload_button = st.button('アップロードする')
  
  if upload_button:
    snowparkDf=session.write_pandas(df,file.name,auto_create_table = True, overwrite=True)
