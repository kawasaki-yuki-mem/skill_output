import streamlit as st
import pandas as pd
import json
from snowflake.snowpark import Session

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
  
  if uploaded_file is not None:
    st.sidebar.write("###")
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
    if df.duplicated().sum().sum() == 0:
      st.success('重複行がありません', icon="✅")
    else:
      df.drop_duplicates(subset=[st.selectbox("選択してください", df.columns)],keep='first', inplace=True)
      st.dataframe(df)


    st.write("##### 欠損値処理")
    if df.isnull().sum().sum() == 0:
      st.success('欠損値がありません', icon="✅")
    else:
      miss_drop = st.selectbox("選択してください", ['列ごと削除', 'ゼロ埋め', '平均値埋め'])
      # if del_button or zero_button or mean_button:
      if miss_drop == '列ごと削除':
        etl_df = df.dropna(axis=0)
        st.dataframe(etl_df)
      elif miss_drop == 'ゼロ埋め':
        etl_df = df.fillna(0)
        st.dataframe(etl_df)    
      elif miss_drop == '平均値埋め':
        etl_df = df.fillna(df.mean(numeric_only=True))
        st.dataframe(etl_df)
  
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
      
  
  # 4. 各データの分布/割合を確認
  st.write("#")
  st.subheader("4. 各データの分布を確認")

  if uploaded_file is not None:
    # st.line_chart(df.select_dtypes(include='number'))
  
    if df.duplicated().sum().sum() == 0 and df.isnull().sum().sum() == 0:
      viz_org = st.selectbox("選択してください", ['折れ線グラフ', '面グラフ', '棒グラフ', '散布図'])
      # x_list_org = st.multiselect('x軸のカラムを選択してください'
      #                              , df.columns)
      # y_list_org = st.multiselect('y軸のカラムを選択してください'
      #                              , df.columns)

      if viz_org == '折れ線グラフ':
        num_col_lst = st.multiselect('カラムを選択してください', df.select_dtypes(include='number').columns)
        st.line_chart(df[num_col_lst])
        
      elif viz_org == '面グラフ':
        num_col_lst = st.multiselect('カラムを選択してください', df.select_dtypes(include='number').columns)
        select_stack = st.selectbox('スタックを選択してください', [None, True, "normalize", "center"])
        st.area_chart(df[num_col_lst], stack=select_stack)
        
      elif viz_org == '棒グラフ':
        num_col_lst = st.multiselect('カラムを選択してください', df.select_dtypes(include='number').columns)
        select_stack = st.selectbox('スタックを選択してください', [None, False, "layered", "normalize", "center"])
        select_horize = st.selectbox('水平に表示しますか', [False, True])
        obj_col_lst = st.multiselect('カラムを選択してください', df.select_dtypes(include='object').columns)
        st.bar_chart(df[num_col_lst], y=df[obj_col_lst], horizontal=select_horize, stack=select_stack)
      
      elif viz_org == '散布図':
        num_col_lst = st.multiselect('カラムを選択してください', df.select_dtypes(include='number').columns)
        st.scatter_chart(df[num_col_lst])
        
    else:
      viz_edit = st.selectbox("選択してください", ['折れ線グラフ', '面グラフ', '棒グラフ', '散布図'])
      # x_list_edit = st.multiselect('x軸のカラムを選択してください'
      #                            , etl_df.columns)
      # y_list_edit = st.multiselect('y軸のカラムを選択してください'
      #                            , etl_df.columns)
      
      if viz_edit == '折れ線グラフ':
        col_lst = st.multiselect('カラムを選択してください', etl_df.columns)
        st.line_chart(etl_df[col_lst])
        
      elif viz_edit == '面グラフ':
        
        st.area_chart()
      elif viz_edit == '棒グラフ':
        st.bar_chart()
      elif viz_edit == '散布図':
        st.scatter_chart()
    
  # 5. Snowflakeにデータアップロード
  st.write("#")
  st.subheader("5. Snowflakeにデータアップロード")
  if uploaded_file is not None:
    # Snowflakeにデータアップロードするボタン
    upload_button = st.button('アップロードする')
    
    upload_data = st.text_input("データファイル名を入力してください:")
    
    # データアップロードボタンを押した場合、Snowflakeにデータアップロード
    if upload_button:
      # Snowflakeの資格情報を読み取る
      with open('creds.json') as f:
        connection_parameters = json.load(f)  
      session = Session.builder.configs(connection_parameters).create()

      # Snowflakeにデータアップロード
      if df.duplicated().sum().sum() == 0 and df.isnull().sum().sum() == 0:
        snowparkDf=session.write_pandas(df, upload_data, auto_create_table = True, overwrite=True)
        st.success('アップロード完了!', icon="✅")
        
      elif etl_df is not None:
        snowparkDf=session.write_pandas(etl_df, upload_data, auto_create_table = True, overwrite=True)
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
# select_box　https://shoblog.iiyan.net/streamlit-selectbox/#st-toc-h-2
# 原点　https://qiita.com/wgsbt4859/items/071de4b8cb4306c8ceec
# 欠損値埋め　https://note.nkmk.me/python-pandas-nan-fillna/#nan-ffill-bfill
# 重複削除　https://pythonandai.com/pandas-duplicate/
# 豊富なグラフ　https://shoblog.iiyan.net/how-to-create-a-graph-with-streamlit/

