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
  
  if uploaded_file is not None:
    st.sidebar.write("### ETL処理前")
    st.sidebar.write(f"### サンプルサイズ:  {df.shape[0]}")
    st.sidebar.write(f"### カラム数     :  {df.shape[1]}")
    # 欠損値の表示
    st.sidebar.write("### 欠損値")
    st.sidebar.write("各カラムの欠損値")
    null_df = pd.DataFrame(df.isnull().sum(), columns=["null"])
    st.sidebar.dataframe(null_df)
    st.sidebar.write(f"### 合計欠損値数  :  {df.isnull().sum().sum()}")
    st.sidebar.write(f"### 重複行の数  :  {df.duplicated().sum().sum()}")
    

  # 要約統計量の表示
  st.write("###")
  st.write("##### 要約統計量 (数値データのみ)")
  st.dataframe(df.describe())

  # 3. ETL処理
  st.write("#")
  st.subheader("3. ETL処理")
  if uploaded_file is not None:
    if df.duplicated().sum().sum() == 0:
      st.success('重複行がありません', icon="✅")
    else:
      st.write("##### 重複処理")
      df.drop_duplicates(subset=[st.selectbox("選択してください", df.columns)],keep='first', inplace=True)
      st.dataframe(df)
    
    if df.isnull().sum().sum() == 0:
      st.success('欠損値がありません', icon="✅")
    else:
      st.write("##### 欠損値処理")
      del_button = st.button('列ごと削除')
      zero_button = st.button('ゼロ埋め')
      mean_button = st.button('平均値埋め')
      if del_button:
        etl_df = df.dropna(axis=0)
        st.dataframe(etl_df)
      elif zero_button:
        etl_df = df.fillna(0)
        st.dataframe(etl_df)    
      elif mean_button:
        etl_df = df.fillna(df.mean(numeric_only=True))
        st.dataframe(etl_df)
    if etl_df is not None:
      st.sidebar.write("##")
      st.sidebar.write("### ETL処理後")
      st.sidebar.write(f"### サンプルサイズ:  {etl_df.shape[0]}")
      st.sidebar.write(f"### カラム数     :  {etl_df.shape[1]}")
      # 欠損値の表示
      st.sidebar.write("### 欠損値")
      st.sidebar.write("各カラムの欠損値")
      null_etl_df = pd.DataFrame(etl_df.isnull().sum(), columns=["null"])
      st.sidebar.dataframe(null_etl_df)
      st.sidebar.write(f"### 合計欠損値数  :  {etl_df.isnull().sum().sum()}")
      st.sidebar.write(f"### 重複行の数  :  {etl_df.duplicated().sum().sum()}")
    else
      st.sidebar.write("### ETL処理後")
      st.sidebar.write(f"### サンプルサイズ:  {df.shape[0]}")
      st.sidebar.write(f"### カラム数     :  {df.shape[1]}")
      # 欠損値の表示
      st.sidebar.write("### 欠損値")
      st.sidebar.write("各カラムの欠損値")
      null_df = pd.DataFrame(df.isnull().sum(), columns=["null"])
      st.sidebar.dataframe(null_df)
      st.sidebar.write(f"### 合計欠損値数  :  {df.isnull().sum().sum()}")
      st.sidebar.write(f"### 重複行の数  :  {df.duplicated().sum().sum()}")
  # 4. 各データの分布/割合を確認
  st.write("#")
  st.subheader("4. 各データの分布を確認")

  # 5. Snowflakeにデータアップロード
  st.write("#")
  st.subheader("5. Snowflakeにデータアップロード")
  if uploaded_file is not None:
    # Snowflakeにデータアップロードするボタン
    upload_button = st.button('アップロードする')
    
    # データアップロードボタンを押した場合、Snowflakeにデータアップロード
    if upload_button:
      # Snowflakeの資格情報を読み取る
      with open('creds.json') as f:
        connection_parameters = json.load(f)  
      session = Session.builder.configs(connection_parameters).create()
    
      # Snowflakeにデータアップロード
      snowparkDf=session.write_pandas(df,file.name,auto_create_table = True, overwrite=True)
      st.success('アップロード完了!', icon="✅")

except:
    st.error(
      """
      エラーが発生しました。
      """
    )


# 参考
# Snowflakeにデータアップロード　https://blog.streamlit.io/build-a-snowflake-data-loader-on-streamlit-in-only-5-minutes/
# サイドメニュー　https://qiita.com/sumikei11/items/e3a567e69c7a86abeaa2
