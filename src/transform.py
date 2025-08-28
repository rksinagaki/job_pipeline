import pandas as pd
import os
import ast
import numpy as np
import boto3
from io import StringIO
import sqlalchemy


# -----------------
# RawData読み込み
# -----------------
df = pd.read_csv('s3://myproject-row-data1/all_pages.csv')

# -----------------
# コラム厳選
# -----------------
columns_to_keep = [
    'job_offer_id',
    'job_offer_name',
    'client',
    'job_offer_areas',
    'job_offer_min_salary',
    'job_offer_max_salary',
    'job_offer_skill_names'
]

df_filtered = df.loc[:, columns_to_keep].copy()

def to_dict(value):
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except Exception:
            return None
    if isinstance(value, float) and pd.isna(value):
        return None
    return None

def to_list(value):
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except Exception:
            return None
    if isinstance(value, float) and pd.isna(value):
        return None
    return None


# -----------------
# データ整形
# -----------------
if 'client' in df_filtered.columns:
    df_filtered['client_dict'] = df_filtered['client'].apply(to_dict)
    df_filtered['client_name'] = [d.get('name') for d in df_filtered['client_dict']]
    df_filtered['employee_count'] = [d.get('employee_count') for d in df_filtered['client_dict']]
    df_filtered['established'] = [d.get('established_at') for d in df_filtered['client_dict']]
    df_filtered['established'] = pd.to_datetime(df_filtered['established'], unit='s')
    df_filtered = df_filtered.drop(['client', 'client_dict'], axis=1, errors='ignore')

if 'job_offer_skill_names' in df_filtered.columns:
    df_filtered['job_offer_skill_names'] = df_filtered['job_offer_skill_names'].apply(to_list)

if 'job_offer_areas' in df_filtered.columns:
    df_filtered['job_offer_areas'] = df_filtered['job_offer_areas'].apply(to_list)

if 'job_offer_name' in df_filtered.columns:
    df_filtered['job_tag'] = 'その他'
    df_filtered.loc[df_filtered['job_offer_name'].str.contains('データ基盤エンジニア|データエンジニア|データベースエンジニア|データ分析基盤エンジニア|DBエンジニア'), 'job_tag'] = 'データエンジニア'
    df_filtered.loc[df_filtered['job_offer_name'].str.contains('データサイエンティスト|データアナリスト'), 'job_tag'] = 'データサイエンティスト'
    df_filtered.loc[df_filtered['job_offer_name'].str.contains('AIエンジニア|機械学習エンジニア|AI開発エンジニア|LLMエンジニア|MLエンジニア'), 'job_tag'] = 'AIエンジニア'

# 欠損値中央値補完
if 'job_offer_max_salary' in df_filtered.columns:
    median_salary = df_filtered['job_offer_max_salary'].median()
    df_filtered['job_offer_max_salary'] = df_filtered['job_offer_max_salary'].replace(0, np.nan)
    df_filtered['job_offer_max_salary'] = df_filtered['job_offer_max_salary'].fillna(median_salary)

if 'job_offer_max_salary' in df_filtered.columns and 'job_offer_min_salary' in df_filtered.columns:
    df_filtered['avg_salary'] = (df_filtered['job_offer_max_salary'] + df_filtered['job_offer_min_salary'])/2

new_order = [
    'job_offer_id',
    'client_name',
    'job_offer_name',
    'job_tag',
    'job_offer_areas',
    'job_offer_skill_names',
    'employee_count',
    'established',
    'job_offer_min_salary',
    'job_offer_max_salary',
    'avg_salary',
]

df_filtered = df_filtered[new_order]

# ーーーーーーー
# S3に保存
# ーーーーーーー
csv_buffer = StringIO()
df_filtered.to_csv(csv_buffer, index=False, encoding='utf-8')

# S3クライアントを初期化
s3_client = boto3.client('s3')

# FilterDataをS3へアップ
bucket_name = 'myproject-row-data1 '
file_key = 'filtered.csv'

try:
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=csv_buffer.getvalue()
    )
    print(f"S3に加工済みデータをアップロードしました: s3://{bucket_name}/{file_key}")
except Exception as e:
    print(f"アップロードに失敗しました: {e}")


# -----------------
# FilterData読み込み
# -----------------
# RDSの接続情報を環境変数から取得
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = 5432

# S3からデータを直接DataFrameに読み込む
bucket_name = 'myproject-row-data1'
file_key = 'filtered.csv'
s3_client = boto3.client('s3')

try:
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_csv(obj['Body'])
    print("S3からデータの読み込みが完了しました。")
except Exception as e:
    print(f"S3からデータの読み込み中にエラーが発生しました: {e}")
    # エラーが発生した場合はここでスクリプトを終了
    exit()

# RDSへの接続URLを作成
db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sqlalchemy.create_engine(db_url)

# RDSへデータを直接書き出す
table_name = 'job_offers_filtered'

try:
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"'{table_name}' テーブルへのデータの書き出しが完了しました。")
except Exception as e:
    print(f"RDSへのデータの書き出し中にエラーが発生しました: {e}")