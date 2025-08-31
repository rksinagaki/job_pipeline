#-----------------
# Lambdaへの記載
#-----------------
import requests
import csv
import json
import os
import ast
import time
import boto3
import io

# APIのURLとヘッダー情報
api_url = "https://www.green-japan.com/api/v2/user/search_job_offers"
headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Referer': 'https://www.green-japan.com/search',
    'X-Requested-With': 'XMLHttpRequest'
}

# 職種タグ
job_tag_ids = [60, 61, 62]

def process_page_response(data, offset, list):
    job_offers_list = data.get("job_offers")
    list.extend(job_offers_list)
    print(f"[page={offset}] 取得（{len(job_offers_list)}件）。")
    next_offset_value = data.get("next_offset")
    return (next_offset_value if isinstance(next_offset_value, int) else None), False

def lambda_handler(event, context):
    # 求人情報リスト
    all_job_offers = []

    # ページネーション
    offset = 0

    while True:
        payload = {
            "offset": offset,
            "order_type": "jobOfferScore",
            "user_search": {
                "job_tag_ids": job_tag_ids
            }
        }

        try:
            response = requests.post(api_url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

            result = process_page_response(data, offset, all_job_offers)
            next_offset_value = result[0]
            is_last = result[1]
            if is_last:
                break
            if next_offset_value is not None:
                offset = next_offset_value
            else:
                offset += 1

            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            try:
                # status_codeの取得
                if hasattr(e, 'response') and e.response is not None:
                    status_code = e.response.status_code
                else:
                    status_code = None

                # bodyの取得
                if hasattr(e, 'response') and e.response is not None:
                    body = e.response.text[:500]
                else:
                    body = ""

                print(f"APIリクエスト中にエラーが発生しました: {e} | status={status_code} | body={body}")
            
            except Exception:
                print(f"APIリクエスト中にエラーが発生しました: {e}")
            break

    # ーーーーーーーーーー
    # S3に接続
    # ーーーーーーーーーー
    s3_client = boto3.client('s3')
    bucket_name = 'myproject-row-data1'
    file_key = 'all_pages.csv'

    # CSVデータをメモリ上で作成
    output = io.StringIO()

    # データが空でなければヘッダーとデータを書き込む
    if all_job_offers:
        fieldnames = list(all_job_offers[0].keys())
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(all_job_offers)

    # 作成したCSV文字列を取得
    csv_string = output.getvalue()

    # S3にアップロード
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_string.encode('utf-8-sig'),
            ContentType='text/csv'
        )
        print(f"\n合計 {len(all_job_offers)} 件の求人情報をS3バケット '{bucket_name}' の '{file_key}' にアップロードしました。")
    except Exception as e:
        print(f"S3へのアップロード中にエラーが発生しました: {e}")