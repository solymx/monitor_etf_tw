import requests
import pandas as pd
from datetime import datetime
import os
import shutil
import traceback

# --- 設定區 ---
API_URL = "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback"
FUND_ID = "399"   # ⚠️ 請確認代號 (399=00929, 00982請自行填入正確代號)
FILE_NAME = "982a" # 固定檔名 (會產生 982a.csv 和 982a.html)
BACKUP_FOLDER = "982a_backup" # 備份舊檔案的資料夾名稱

# 定義固定檔名路徑
CSV_FILE_PATH = f"{FILE_NAME}.csv"
HTML_FILE_PATH = f"{FILE_NAME}.html"

# Payload
payload = {
    "fundId": FUND_ID,
    "date": None 
}

# Headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Referer": "https://www.capitalfund.com.tw/"
}

def get_previous_csv():
    """
    直接讀取當前目錄下的固定檔名 CSV 作為舊資料
    """
    if os.path.exists(CSV_FILE_PATH):
        print(f"🔎 找到舊資料進行比對: {CSV_FILE_PATH}")
        return CSV_FILE_PATH
    else:
        print("🔎 目前沒有舊資料，將視為首次執行。")
        return None

def analyze_changes(today_df, prev_file_path):
    """
    比對今日與昨日持股，產生狀態欄位
    """
    if not prev_file_path:
        today_df['狀態'] = '🆕 首次抓取'
        today_df['股數變化'] = 0
        return today_df

    try:
        # 讀取舊檔案 (指定字串避免 0050 變 50)
        prev_df = pd.read_csv(prev_file_path, dtype={'股票代號': str})
        prev_df['股票代號'] = prev_df['股票代號'].astype(str).str.strip()

        prev_df = prev_df[['股票代號', '持有股數', '股票名稱']]
        prev_df.columns = ['股票代號', '昨日股數', '昨日名稱']
    except Exception as e:
        print(f"⚠️ 讀取舊檔案失敗 ({e})，略過比對")
        today_df['狀態'] = '-'
        return today_df

    # 合併比對
    merged_df = pd.merge(today_df, prev_df, on='股票代號', how='outer')
    merged_df['股票名稱'] = merged_df['股票名稱'].fillna(merged_df['昨日名稱'])

    merged_df['持有股數'] = merged_df['持有股數'].fillna(0)
    merged_df['昨日股數'] = merged_df['昨日股數'].fillna(0)
    merged_df['股數變化'] = merged_df['持有股數'] - merged_df['昨日股數']

    def determine_status(row):
        if row['昨日股數'] == 0 and row['持有股數'] > 0:
            return "🔥 新進"
        elif row['持有股數'] == 0 and row['昨日股數'] > 0:
            return "👋 賣出"
        elif row['股數變化'] > 0:
            return "🔺 增加"
        elif row['股數變化'] < 0:
            return "🔻 減少"
        else:
            return "➖ 持平"

    merged_df['狀態'] = merged_df.apply(determine_status, axis=1)

    final_df = merged_df[['股票代號', '股票名稱', '權重(%)', '持有股數', '股數變化', '狀態']]
    final_df = final_df.sort_values(by=['權重(%)'], ascending=False, na_position='last')
    
    return final_df

def backup_old_files():
    """
    在覆蓋檔案之前，先將現有的 982a.csv / 982a.html 備份起來
    備份檔名會加上 '修改日期'
    """
    if not os.path.exists(BACKUP_FOLDER):
        os.makedirs(BACKUP_FOLDER)

    for file_path in [CSV_FILE_PATH, HTML_FILE_PATH]:
        if os.path.exists(file_path):
            # 取得檔案最後修改時間來當作檔名日期
            mod_time = os.path.getmtime(file_path)
            date_str = datetime.fromtimestamp(mod_time).strftime("%Y%m%d")
            
            # 備份檔名例如: 982a_backup/20231215_982a.csv
            file_ext = os.path.splitext(file_path)[1]
            backup_name = f"{date_str}_{FILE_NAME}{file_ext}"
            destination = os.path.join(BACKUP_FOLDER, backup_name)

            try:
                # 這裡改用 copy 還是 move? 
                # 建議 move，因為等一下主程式會產生新的同名檔案
                shutil.move(file_path, destination)
                print(f"📦 已將舊檔備份至: {destination}")
            except Exception as e:
                print(f"⚠️ 備份失敗 {file_path}: {e}")

def save_html(df, file_path, title_date):
    """
    存成 HTML
    """
    def color_status(val):
        color = 'black'
        weight = 'normal'
        if '新進' in val: color = 'red'; weight = 'bold'
        elif '增加' in val: color = '#d9534f'
        elif '減少' in val: color = 'green'
        elif '賣出' in val: color = 'gray'; weight = 'bold'
        return f'color: {color}; font-weight: {weight}'

    def row_style(row):
        if '賣出' in row['狀態']:
            return ['background-color: #f9f9f9; color: #999'] * len(row)
        return [''] * len(row)

    try:
        styler = df.style.map(color_status, subset=['狀態'])
    except AttributeError:
        styler = df.style.applymap(color_status, subset=['狀態'])

    styler = styler.apply(row_style, axis=1)\
                   .format({'權重(%)': "{:.2f}", '持有股數': "{:,.0f}", '股數變化': "{:+,.0f}"})
    
    html_content = styler.to_html()

    html_template = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>ETF 持股監控 - {title_date}</title>
        <style>
            body {{ font-family: "Microsoft JhengHei", Arial, sans-serif; margin: 20px; background-color: #fdfdfd; }}
            h2 {{ color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }}
            table {{ border-collapse: collapse; width: 100%; max-width: 900px; margin-top: 15px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
            th {{ background-color: #007bff; color: white; padding: 12px; text-align: left; }}
            td {{ border-bottom: 1px solid #ddd; padding: 10px; }}
            tr:hover {{ background-color: #f1f1f1; }}
        </style>
    </head>
    <body>
        <h2>📊 {FILE_NAME} 持股變化日報 ({title_date})</h2>
        {html_content}
        <p style="color: #666; font-size: 0.9em;">資料產生時間: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    </body>
    </html>
    """
    
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_template)

def main():
    print(f"🚀 開始抓取 ETF 代號 {FUND_ID} 的持股資料...")
    
    try:
        response = requests.post(API_URL, json=payload, headers=headers)
        
        if response.status_code == 200:
            raw_data = response.json()
            
            if 'data' in raw_data and 'stocks' in raw_data['data']:
                stock_list = raw_data['data']['stocks']
                
                if stock_list:
                    # 1. 轉成 DataFrame 並清洗
                    df = pd.DataFrame(stock_list)
                    df = df[['stocNo', 'stocName', 'weight', 'shareFormat']]
                    df.columns = ['股票代號', '股票名稱', '權重(%)', '持有股數']
                    df['股票代號'] = df['股票代號'].astype(str).str.strip()
                    df['持有股數'] = df['持有股數'].astype(str).str.replace(',', '').astype(float)
                    
                    # 2. 尋找舊檔案 (固定檔名 982a.csv)
                    prev_csv = get_previous_csv()
                    
                    # 3. 進行比對分析
                    final_df = analyze_changes(df, prev_csv)
                    
                    # 4. 備份舊檔案 (如果有舊的 982a.csv，把它改名移走)
                    backup_old_files()
                    
                    # 5. 儲存最新的 CSV 與 HTML (使用固定檔名)
                    today_str = datetime.now().strftime("%Y-%m-%d")
                    final_df.to_csv(CSV_FILE_PATH, index=False, encoding='utf-8-sig')
                    save_html(final_df, HTML_FILE_PATH, today_str)
                    
                    print(f"\n✅ 完成！")
                    print(f"   - 最新檔案: {CSV_FILE_PATH}")
                    print(f"   - 最新網頁: {HTML_FILE_PATH}")
                    
                    # 顯示變化摘要
                    changes = final_df[final_df['狀態'].isin(['🔥 新進', '👋 賣出', '🔺 增加', '🔻 減少'])]
                    if not changes.empty:
                        print(f"\n📢 今日異動 ({len(changes)} 筆):")
                        print(changes[['股票名稱', '狀態', '股數變化']].to_string(index=False))
                    else:
                        print("\n💤 今日持股無變化")

                else:
                    print("⚠️ API 回傳的 'stocks' 列表是空的。")
            else:
                print("⚠️ 資料結構異常。")
        else:
            print(f"❌ 請求失敗: {response.status_code}")

    except Exception as e:
        print(f"❌ 發生錯誤: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
