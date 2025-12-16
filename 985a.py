import requests
import pandas as pd
import os
import shutil
from datetime import datetime, date
import traceback # 引入這個以便查看錯誤細節

# ==========================================
# 1. 設定區
# ==========================================
FOLDER_NAME = "985a"            # 備份資料夾名稱
CSV_FILENAME = "985a.csv"       # 最新數據 CSV
HTML_FILENAME = "985a.html"     # 產出的報表名稱
API_URL = "https://www.nomurafunds.com.tw/API/ETFAPI/api/Fund/GetFundAssets"

# 設定查詢日期 (正式運行請使用 date.today())
SEARCH_DATE = str(date.today()) 
#SEARCH_DATE = "2025-12-15" # 測試用 (若要測試特定日期可打開此行)

PAYLOAD = {
    "FundID": "00985A",
    "SearchDate": SEARCH_DATE 
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0",
    "Content-Type": "application/json",
    "Referer": "https://www.nomurafunds.com.tw/",
    "Origin": "https://www.nomurafunds.com.tw"
}

# ==========================================
# 2. 核心功能函式
# ==========================================

def fetch_data():
    """抓取 API 資料並整理成 DataFrame"""
    print(f"正在請求資料... 日期: {SEARCH_DATE}")
    try:
        response = requests.post(API_URL, headers=HEADERS, json=PAYLOAD)
        if response.status_code == 200:
            data = response.json()
            tables = data.get('Entries', {}).get('Data', {}).get('Table', [])
            stock_data = next((t for t in tables if t['TableTitle'] == '股票'), None)
            
            if stock_data:
                columns = [col['Name'] for col in stock_data['Columns']]
                df = pd.DataFrame(stock_data['Rows'], columns=columns)
                
                # ---【修正重點開始】數值清洗邏輯 ---
                # 移除逗號並轉為數字，解決 FutureWarning
                for col in df.columns:
                    # 只要欄位名稱包含這些關鍵字，就進行轉換
                    if any(x in col for x in ['股數', '權重', '數', '值']):
                        try:
                            # 1. 先轉字串並移除逗號
                            df[col] = df[col].astype(str).str.replace(',', '')
                            # 2. 強制轉數字 (無法轉的變成 NaN)
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                            # 3. 將 NaN 補為 0 (避免後續計算出錯)
                            df[col] = df[col].fillna(0)
                        except Exception as e:
                            print(f"欄位 {col} 轉換數值時發生錯誤: {e}")
                # ---【修正重點結束】---

                return df
            else:
                print("錯誤: 找不到股票資料表 (可能是假日或無資料)")
                return None
        else:
            print(f"API 請求失敗: {response.status_code}")
            return None
    except Exception as e:
        print(f"抓取發生錯誤: {e}")
        return None

def process_comparison(df_new):
    """處理備份與資料比對"""
    
    # 建立資料夾
    if not os.path.exists(FOLDER_NAME):
        os.makedirs(FOLDER_NAME)

    # 確保新資料的 Key 是字串
    key_col = '股票代號'
    if key_col in df_new.columns:
        df_new[key_col] = df_new[key_col].astype(str).str.strip()

    df_final = df_new.copy()
    
    # 檢查是否有舊檔
    if os.path.exists(CSV_FILENAME):
        print("發現舊資料，進行比對...")
        try:
            # 讀取 CSV
            df_old = pd.read_csv(CSV_FILENAME)
            
            # 確保舊資料的 Key 是字串
            if key_col in df_old.columns:
                df_old[key_col] = df_old[key_col].astype(str).str.strip()
            
            val_col = '股數' 
            
            # 合併比對 (Outer Join 保留所有變動)
            merged = pd.merge(
                df_new, 
                df_old[[key_col, val_col]], 
                on=key_col, 
                how='outer', 
                suffixes=('', '_old')
            )
            
            # 填充 NaN
            merged[val_col] = merged[val_col].fillna(0)
            merged[f'{val_col}_old'] = merged[f'{val_col}_old'].fillna(0)
            
            # 計算差異
            merged['股數變化'] = merged[val_col] - merged[f'{val_col}_old']
            
            # 判斷狀態
            def get_status(row):
                curr = row[val_col]
                old = row[f'{val_col}_old']
                if old == 0 and curr > 0: return "新買入"
                if old > 0 and curr == 0: return "全部賣出"
                if curr > old: return "加碼"
                if curr < old: return "減碼"
                return "持平"

            merged['狀態'] = merged.apply(get_status, axis=1)
            
            # 若是「全部賣出」，原本的其他欄位會是 NaN，這裡補字
            merged['股票名稱'] = merged['股票名稱'].fillna('已清倉')
            
            df_final = merged
            
            # 備份舊檔
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(FOLDER_NAME, f"backup_{timestamp}.csv")
            shutil.move(CSV_FILENAME, backup_path)
            print(f"舊檔已備份至: {backup_path}")
            
        except Exception as e:
            traceback.print_exc()
            print(f"比對過程錯誤 (將略過比對): {e}")
            df_final['狀態'] = '比對失敗'
            df_final['股數變化'] = 0
    else:
        print("無舊資料，首次執行。")
        df_final['狀態'] = '首次建立'
        df_final['股數變化'] = 0
        
    # 存新檔
    df_final.to_csv(CSV_FILENAME, index=False, encoding='utf-8-sig')
    return df_final

def generate_html_report(df):
    """將 DataFrame 轉換為美觀的 HTML 檔案"""
    
    table_rows = ""
    for index, row in df.iterrows():
        status = row.get('狀態', '未知')
        change = row.get('股數變化', 0)
        
        # 狀態標籤顏色
        badge_class = "bg-secondary"
        if "新買" in status or "加碼" in status: badge_class = "bg-danger"
        elif "賣出" in status or "減碼" in status: badge_class = "bg-success"
        
        # 數值顏色
        text_class = ""
        change_str = "-"
        if change > 0:
            text_class = "text-danger fw-bold"
            change_str = f"▲ {int(change):,}"
        elif change < 0:
            text_class = "text-success fw-bold"
            change_str = f"▼ {int(change):,}"
            
        try:
            shares = f"{int(row['股數']):,}"
        except:
            shares = str(row['股數'])
            
        weight = row.get('權重', '-') 
        
        table_rows += f"""
        <tr>
            <td><span class="badge {badge_class}">{status}</span></td>
            <td>{row['股票代號']}</td>
            <td>{row['股票名稱']}</td>
            <td class="text-end">{shares}</td>
            <td class="text-end {text_class}">{change_str}</td>
            <td class="text-end">{weight}%</td>
        </tr>
        """

    html_content = f"""
    <!DOCTYPE html>
    <html lang="zh-TW">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>00985A 持股追蹤日報</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            body {{ background-color: #f0f2f5; padding: 20px; font-family: "Microsoft JhengHei", sans-serif; }}
            .container {{ background-color: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); }}
            h2 {{ color: #333; font-weight: bold; }}
            .footer {{ margin-top: 20px; font-size: 0.85em; color: #888; text-align: right; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="d-flex justify-content-between align-items-center mb-4">
                <h2>📊 00985A 持股變動追蹤</h2>
                <span class="badge bg-primary fs-6">資料日期: {SEARCH_DATE}</span>
            </div>
            
            <div class="table-responsive">
                <table class="table table-hover align-middle">
                    <thead class="table-dark">
                        <tr>
                            <th>狀態</th>
                            <th>代號</th>
                            <th>名稱</th>
                            <th class="text-end">持有股數</th>
                            <th class="text-end">較昨日增減</th>
                            <th class="text-end">權重</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows}
                    </tbody>
                </table>
            </div>
            <div class="footer">
                報表生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </div>
        </div>
    </body>
    </html>
    """
    
    with open(HTML_FILENAME, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    print(f"報表已生成: {HTML_FILENAME}")

# ==========================================
# 3. 主程式執行
# ==========================================
if __name__ == "__main__":
    df = fetch_data()
    
    if df is not None and not df.empty:
        df_processed = process_comparison(df)
        generate_html_report(df_processed)
    else:
        print("程式結束 (無資料更新)")
