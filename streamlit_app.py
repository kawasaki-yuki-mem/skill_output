import streamlit as st
import pandas as pd
import json
from snowflake.snowpark import Session

# # connect to Snowflake
# with open('creds.json') as f:
#   connection_parameters = json.load(f)  
# session = Session.builder.configs(connection_parameters).create()


# ファイルアップロード
file = st.file_uploader("アクセスログをアップロードしてください。", type={"csv"})

# データを閲覧するボタン
orgdata_view = st.button('データを閲覧する')

# データ閲覧ボタンを押した場合
if orgdata_view:
  file_df = pd.read_csv(file)
  st.write(file_df)

  upload_button = st.button('アップロードする')
  
  if upload_button:
    # connect to Snowflake
    with open('creds.json') as f:
      connection_parameters = json.load(f)  
    session = Session.builder.configs(connection_parameters).create()
    file_df = pd.read_csv(file)
    snowparkDf=session.write_pandas(file_df,file.name,auto_create_table = True, overwrite=True)
    st.success('アップロード完了!', icon="✅")
