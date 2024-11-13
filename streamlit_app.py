import streamlit as st
import pandas as pd
import json
from snowflake.snowpark import Session

# データアップロードの説明
st.markdown(""" 
## データをテーブル表示
- 「Browse files」をクリックしてローカルにあるデータファイルを指定してアップロードしてください。  
- 「データを閲覧する」をクリックしてアップロードされたデータファイルをテーブルで表示してください。
""")

# ファイルアップロード
file = st.sidebar.file_uploader("データファイルをアップロードしてください。", type={"csv"})

# データを閲覧するボタン
orgdata_view = st.sidebar.button('データを閲覧する')

# データ閲覧ボタンを押した場合、データ閲覧
if orgdata_view:
  file_df = pd.read_csv(file)
  st.write(file_df)

file_df = pd.read_csv(file)
columns = st.selectbox("選択してください。", file_df.columns)








# Snowflakeにデータアップロードするボタン
upload_button = st.sidebar.button('アップロードする')

# データアップロードボタンを押した場合、Snowflakeにデータアップロード
if upload_button:
  
  # Snowflakeの資格情報を読み取る
  with open('creds.json') as f:
    connection_parameters = json.load(f)  
  session = Session.builder.configs(connection_parameters).create()

  # # アップロードするデータ
  # file_df = pd.read_csv(file)

  # Snowflakeにデータアップロード
  snowparkDf=session.write_pandas(file_df,file.name,auto_create_table = True, overwrite=True)
  st.success('アップロード完了!', icon="✅")

# 参考
# Snowflakeにデータアップロード　https://blog.streamlit.io/build-a-snowflake-data-loader-on-streamlit-in-only-5-minutes/
# サイドメニュー　https://qiita.com/sumikei11/items/e3a567e69c7a86abeaa2
