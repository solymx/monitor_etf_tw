import requests
import pandas as pd
from datetime import datetime
import os
import glob
import shutil
import traceback

# --- 設定區 ---
API_URL = "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback"
FUND_ID = "399"   # ⚠️ 請確認這是你要抓的基金代號 (399=00929)。如果要抓 00982，請填入正確代號。
FILE_TAG = "00982a"  # 檔名識別字 (生成的檔案會是 YYYYMMDD_00982a.csv)
BACKUP_FOLDER = "982a" # 備份舊檔案的資料夾名稱

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
    尋找當前目錄下最近的一份舊 CSV 檔案
    """
    # 找尋所有符合格式的 CSV
    csv_files = glob.glob(f"*_{FILE_TAG}.csv")
    
    # 如果沒有任何檔案，回傳 None
    if not csv_files:
        return None
    
    # 根據檔名排序 (日期在前面，所以排序最後一個就是最近日期的)
    csv_files.sort()
    
    # 取出最後一個檔案
    latest_file = csv_files[-1]
    
    print(f"🔎 找到上一份資料進行比對: {latest_file}")
    return latest_file

def analyze_changes(today_df, prev_file_path):
    """
    比對今日與昨日持股，產生狀態欄位
    """
    if not prev_file_path:
        # 如果沒有舊檔案，所有股票都算 "新資料"
        today_df['狀態'] = '🆕 首次抓取'
        today_df['股數變化'] = 0
        return today_df

    # 讀取舊檔案
    try:
        # 🟢【修正點 1】讀取時指定 '股票代號' 為字串，避免 0050 變成 50
        prev_df = pd.read_csv(prev_file_path, dtype={'股票代號': str})
        
        # 雙重保險：確保轉為字串並去除空白
        prev_df['股票代號'] = prev_df['股票代號'].astype(str).str.strip()

        # 只取需要的欄位來比對
        prev_df = prev_df[['股票代號', '持有股數', '股票名稱']]
        prev_df.columns = ['股票代號', '昨日股數', '昨日名稱'] # 改名避免衝突
    except Exception as e:
        print(f"⚠️ 讀取舊檔案失敗 ({e})，略過比對")
        today_df['狀態'] = '-'
        return today_df

    # --- 關鍵步驟：合併 (Outer Join) 以包含賣出的股票 ---
    # 🟢 現在兩邊的 '股票代號' 都是字串 (Object)，可以安全合併了
    merged_df = pd.merge(today_df, prev_df, on='股票代號', how='outer')

    # 填補名稱：如果是「賣出」的股票，today_df 的股票名稱會是 NaN，要用舊檔案的名稱補回來
    merged_df['股票名稱'] = merged_df['股票名稱'].fillna(merged_df['昨日名稱'])

    # 計算變化
    merged_df['持有股數'] = merged_df['持有股數'].fillna(0) # 今天沒股數 = 0
    merged_df['昨日股數'] = merged_df['昨日股數'].fillna(0) # 昨天沒股數 = 0
    merged_df['股數變化'] = merged_df['持有股數'] - merged_df['昨日股數']

    # 定義狀態判斷函式
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

    # 整理欄位 (移除輔助用的欄位)
    final_df = merged_df[['股票代號', '股票名稱', '權重(%)', '持有股數', '股數變化', '狀態']]
    
    # 排序：持有的排上面 (權重高到低)，賣出的排最後
    final_df = final_df.sort_values(by=['權重(%)'], ascending=False, na_position='last')
    
    return final_df

def manage_backups(today_filename):
    """
    將非今日的舊 CSV 檔案移動到備份資料夾
    """
    # 確保備份資料夾存在
    if not os.path.exists(BACKUP_FOLDER):
        os.makedirs(BACKUP_FOLDER)
        # print(f"📁 建立備份資料夾: {BACKUP_FOLDER}")

    # 搜尋根目錄下的目標 CSV
    files = glob.glob(f"*_{FILE_TAG}.csv")
    
    for file in files:
        # 如果這個檔案 "不是" 今天要產生的檔案，就搬進去
        if file != today_filename:
            destination = os.path.join(BACKUP_FOLDER, file)
            # 如果備份資料夾已經有同名檔案，先刪除舊的以避免報錯
            if os.path.exists(destination):
                os.remove(destination)
            
            shutil.move(file, destination)
            print(f"📦 已備份舊檔案: {file} -> {BACKUP_FOLDER}/")

def save_html(df, file_path, title_date):
    """
    存成漂亮的 HTML，加入顏色標示
    """
    # 針對狀態做顏色標記的函式 (CSS)
    def color_status(val):
        color = 'black'
        weight = 'normal'
        if '新進' in val: color = 'red'; weight = 'bold'
        elif '增加' in val: color = '#d9534f' # 紅色系
        elif '減少' in val: color = 'green'
        elif '賣出' in val: color = 'gray'; weight = 'bold'
        return f'color: {color}; font-weight: {weight}'

    # 針對整列做背景色的函式 (賣出顯示灰色背景)
    def row_style(row):
        if '賣出' in row['狀態']:
            return ['background-color: #f9f9f9; color: #999'] * len(row)
        return [''] * len(row)

    # 產生 HTML 表格
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
        <h2>📊 {FILE_TAG} 持股變化日報 ({title_date})</h2>
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
                    # 1. 轉成 DataFrame
                    df = pd.DataFrame(stock_list)
                    
                    # 2. 清洗資料
                    df = df[['stocNo', 'stocName', 'weight', 'shareFormat']]
                    df.columns = ['股票代號', '股票名稱', '權重(%)', '持有股數']
                    
                    # 🟢【修正點 2】強制將今日資料的股票代號轉為字串，並去除空白
                    df['股票代號'] = df['股票代號'].astype(str).str.strip()
                    
                    # 轉型態確保計算正確 (移除逗號轉數字)
                    df['持有股數'] = df['持有股數'].astype(str).str.replace(',', '').astype(float)
                    
                    # 3. 尋找舊檔案並進行比對
                    prev_csv = get_previous_csv()
                    final_df = analyze_changes(df, prev_csv)
                    
                    # 4. 準備檔名
                    today_str = datetime.now().strftime("%Y%m%d")
                    csv_filename = f"{today_str}_{FILE_TAG}.csv"
                    html_filename = f"{today_str}_{FILE_TAG}.html"
                    
                    # 5. 檔案管理 (備份舊的 CSV)
                    manage_backups(csv_filename)
                    
                    # 6. 儲存最新的 CSV 與 HTML 到根目錄
                    final_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                    save_html(final_df, html_filename, today_str)
                    
                    print(f"\n✅ 完成！")
                    print(f"   - 最新 CSV: {csv_filename}")
                    print(f"   - 最新 HTML: {html_filename}")
                    print(f"   - 歷史備份: 詳見 {BACKUP_FOLDER}/ 資料夾")
                    
                    # 顯示變化摘要 (在終端機預覽)
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
        # 印出完整錯誤訊息以便除錯
        traceback.print_exc()

if __name__ == "__main__":
    main()
