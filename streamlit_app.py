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

if file is not None:
  # データの閲覧
  df = pd.read_csv(file)
  st.dataframe(df)

  null_df = pd.DataFrame(df.isnull().sum(), columns=["null"])
  st.dataframe(null_df)

  null_df_all = pd.DataFrame(df.isnull(), columns=["null"])
  st.dataframe(null_df_all)
  
  
  # columns = st.selectbox("選択してください。", file_df.columns)








  # Snowflakeにデータアップロードするボタン
  upload_button = st.sidebar.button('アップロードする')
  
  # データアップロードボタンを押した場合、Snowflakeにデータアップロード
  if upload_button:
    
    # Snowflakeの資格情報を読み取る
    with open('creds.json') as f:
      connection_parameters = json.load(f)  
    session = Session.builder.configs(connection_parameters).create()
  
    # Snowflakeにデータアップロード
    snowparkDf=session.write_pandas(df,file.name,auto_create_table = True, overwrite=True)
    st.success('アップロード完了!', icon="✅")

# 参考
# Snowflakeにデータアップロード　https://blog.streamlit.io/build-a-snowflake-data-loader-on-streamlit-in-only-5-minutes/
# サイドメニュー　https://qiita.com/sumikei11/items/e3a567e69c7a86abeaa2
