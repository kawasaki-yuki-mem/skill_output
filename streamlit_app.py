import streamlit as st
import pandas as pd
import json
from snowflake.snowpark import Session

st.write("#")

# 1. データを読み込む
st.subheader("1. データをアップロードする")
uploaded_file=st.file_uploader("csvファイルをアップロードしてください。", type='csv')

if uploaded_file is not None:
  df = pd.read_csv(uploaded_file)
    
  # データの表示
  st.dataframe(df)

try:
  # 2. 要約統計量の確認
  st.write("#")
  st.subheader("2. 要約統計量の確認")
  
  # サイドバーの設定
  st.sidebar.write("# データサンプルサイズ")
  st.sidebar.write("##")
  st.sidebar.write("### 欠損値")
  st.sidebar.write("各カラムの欠損値")
  
  if uploaded_file is not None:
    st.sidebar.write(f"### サンプルサイズ:  {df.shape[0]}")
    st.sidebar.write(f"### カラム数      :  {df.shape[1]}")

    # 欠損値の表示
    null_df = pd.DataFrame(df.isnull().sum(), columns=["null"])
    st.sidebar.dataframe(null_df)
    

    # 要約統計量の表示
    st.write("###")
    st.write("##### 要約統計量 (数値データのみ)")
    
    # TODO: カテゴリ変数に対応したいが、時系列データはdescribeでエラーをはくので、要改善
    # st.write(df.describe())
    st.dataframe(df.describe())

    st.sidebar.write("# データサンプルサイズ")

    # 3. ETL処理
    st.write("#")
    st.subheader("3. ETL処理")

    # 4. 各データの分布/割合を確認
    st.write("#")
    st.subheader("4. 各データの分布を確認")

    # 5. Snowflakeにデータアップロード
    st.write("#")
    st.subheader("5. Snowflakeにデータアップロード")

except:
    st.error(
      """
      エラーが発生しました。
      """
    )







  # Snowflakeにデータアップロードするボタン
  # upload_button = st.sidebar.button('アップロードする')
  
  # # データアップロードボタンを押した場合、Snowflakeにデータアップロード
  # if upload_button:
    
  #   # Snowflakeの資格情報を読み取る
  #   with open('creds.json') as f:
  #     connection_parameters = json.load(f)  
  #   session = Session.builder.configs(connection_parameters).create()
  
  #   # Snowflakeにデータアップロード
  #   snowparkDf=session.write_pandas(df,file.name,auto_create_table = True, overwrite=True)
  #   st.success('アップロード完了!', icon="✅")

# 参考
# Snowflakeにデータアップロード　https://blog.streamlit.io/build-a-snowflake-data-loader-on-streamlit-in-only-5-minutes/
# サイドメニュー　https://qiita.com/sumikei11/items/e3a567e69c7a86abeaa2
