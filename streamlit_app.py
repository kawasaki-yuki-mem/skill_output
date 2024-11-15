import streamlit as st
import pandas as pd
import json
from snowflake.snowpark import Session
import matplotlib.pyplot as plt
import japanize_matplotlib

st.write("#")

# Snowflakeの資格情報を読み取る
with open('creds.json') as f:
  connection_parameters = json.load(f)  
session = Session.builder.configs(connection_parameters).create()

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

  # データ情報、欠損値、重複の表示
  if uploaded_file is not None:
    st.sidebar.write("###")
    st.sidebar.write("### ETL処理前")
    st.sidebar.write(f"### サンプルサイズ:  {df.shape[0]}")
    st.sidebar.write(f"### カラム数     :  {df.shape[1]}")
    
    # 欠損値、重複の表示
    st.sidebar.write("### 欠損値")
    st.sidebar.write("各カラムの欠損値")
    null_df = pd.DataFrame(df.isnull().sum(), columns=["null"])
    st.sidebar.dataframe(null_df)
    st.sidebar.write(f"### 合計欠損値数  :  {df.isnull().sum().sum()}")
    st.sidebar.write(f"### 重複行の数  :  {df.duplicated().sum().sum()}")
    st.sidebar.write("### ーーーーーーーーー")
    
  # 要約統計量の表示
  st.write("###")
  st.write("##### 要約統計量 (数値データのみ)")
  st.dataframe(df.describe())
  
  # 3. ETL処理
  st.write("#")
  st.subheader("3. ETL処理")

  if uploaded_file is not None:
    st.write("##### 重複処理")

    # 重複の有無確認後、重複削除
    if df.duplicated().sum().sum() == 0:
      st.success('重複行がありません', icon="✅") 
      
    else:
      df.drop_duplicates(subset=[st.selectbox("選択してください", df.columns)],keep='first', inplace=True)
      st.dataframe(df)
  
    st.write("##### データ抽出")

    # データ抽出を選択
    if st.selectbox("選択してください", ['はい', 'いいえ']) == 'いいえ':
      st.success('そのままのデータを使用します', icon="✅")
      
    else:
      df = df[st.multiselect('抽出するカラムを選択してください', df.columns)]
      st.dataframe(df)

    # 欠損値の有無確認後、欠損値削除・補完
    st.write("##### 欠損値処理")
    if df.isnull().sum().sum() == 0 or df.isnull().sum().sum() == 0:
      st.success('欠損値がありません', icon="✅")
      
    else:
      miss_drop = st.selectbox("選択してください", ['列ごと削除', 'ゼロ埋め', '平均値埋め'])
      
      if miss_drop == '列ごと削除':
        etl_df = df.dropna(axis=0)
        st.dataframe(etl_df)
        
      elif miss_drop == 'ゼロ埋め':
        etl_df = df.fillna(0)
        st.dataframe(etl_df)   
        
      elif miss_drop == '平均値埋め':
        etl_df = df.fillna(df.mean(numeric_only=True))
        st.dataframe(etl_df)

      # ETL処理後のデータ情報、欠損値、重複の表示
      if etl_df is not None:
        st.sidebar.write("###")
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

    # データ情報、欠損値、重複の表示
    if df.duplicated().sum().sum() == 0 and df.isnull().sum().sum() == 0:
      st.sidebar.write("##")
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
      
  
  # 4. 各データの分布を確認
  st.write("#")
  st.subheader("4. 各データの分布を確認")
  
  if uploaded_file is not None:

    # ETL処理をしていないデータ
    if df.duplicated().sum().sum() == 0 and df.isnull().sum().sum() == 0:
      
      # 表示するグラフとカラムを選択
      viz_org = st.selectbox("選択してください", ['折れ線グラフ', '棒グラフ', '散布図', 'ヒストグラム'])
      xcol_lst = st.selectbox('xカラムを選択してください', df.columns)
      ycol_lst = st.selectbox('yカラムを選択してください', df.columns)

      if viz_org == '折れ線グラフ':
        plt.plot(df[xcol_lst], df[ycol_lst])
        st.pyplot(plt)
        
      elif viz_org == '棒グラフ':
        plt.bar(df[xcol_lst], df[ycol_lst])
        st.pyplot(plt)
      
      elif viz_org == '散布図':
        plt.scatter(df[xcol_lst], df[ycol_lst])
        st.pyplot(plt)
        
      elif viz_org == 'ヒストグラム':
        plt.hist([df[xcol_lst], df[ycol_lst]])
        st.pyplot(plt)

    # ETL処理をしたデータ
    else:

      # 表示するグラフとカラムを選択
      viz_edit = st.selectbox("選択してください", ['折れ線グラフ', '棒グラフ', '散布図', 'ヒストグラム'])
      xcol_lst = st.selectbox('xカラムを選択してください', etl_df.columns)
      ycol_lst = st.selectbox('yカラムを選択してください', etl_df.columns)
      
      if viz_edit == '折れ線グラフ':
        plt.plot(etl_df[xcol_lst], etl_df[ycol_lst])
        st.pyplot(plt)
        
      elif viz_edit == '棒グラフ':
        plt.bar(etl_df[xcol_lst], etl_df[ycol_lst])
        st.pyplot(plt)
        
      elif viz_edit == '散布図':
        plt.scatter(etl_df[xcol_lst], etl_df[ycol_lst])
        st.pyplot(plt)

      elif viz_edit == 'ヒストグラム':
        plt.hist([etl_df[xcol_lst], etl_df[ycol_lst]])
        st.pyplot(plt)
    
  # 5. Snowflakeにデータアップロード
  st.write("#")
  st.subheader("5. Snowflakeにデータアップロード")
  
  if uploaded_file is not None:
    # Snowflakeにアップロードするデータ名とアップロードボタン
    upload_button = st.button('アップロードする')
    upload_data = st.text_input("データファイル名を入力してください:")
    
    # データアップロードボタンを押した場合、Snowflakeにデータアップロード
    if upload_button:
  
      # ETL処理をしていないデータをSnowflakeにデータアップロード
      if df.duplicated().sum().sum() == 0 and df.isnull().sum().sum() == 0:
        snowparkDf=session.write_pandas(df, upload_data, auto_create_table = True, overwrite=True)
        st.success('アップロード完了!', icon="✅")

      # ETL処理をしたデータをSnowflakeにデータアップロード
      elif etl_df is not None:
        snowparkDf=session.write_pandas(etl_df, upload_data, auto_create_table = True, overwrite=True)
        st.success('アップロード完了!', icon="✅")
# 例外処理
except:
    st.error(
      """
      エラーが発生しました。
      """
    )
