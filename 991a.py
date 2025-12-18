import requests
import pandas as pd
import io
import os
import shutil
from datetime import datetime
import glob

def run_daily_update():
    # 1. 設定
    target_etf = "ETF23"
    main_csv = "991a.csv"
    main_html = "991a.html"
    backup_folder = "991a"
    # 自動抓取今天日期
    today_str = datetime.now().strftime("%Y%m%d")
    #today_str = "20251217"
    
    if not os.path.exists(backup_folder):
        os.makedirs(backup_folder)

    # 2. 備份舊檔案
    if os.path.exists(main_csv):
        # 取得舊檔案最後修改時間
        mtime = os.path.getmtime(main_csv)
        file_date = datetime.fromtimestamp(mtime).strftime("%Y%m%d")
        backup_path = os.path.join(backup_folder, f"holdings_{file_date}.csv")
        
        # 避免同檔名覆蓋 (如果是同一天重複執行)
        if os.path.exists(backup_path):
            backup_path = os.path.join(backup_folder, f"holdings_{file_date}_{int(mtime)}.csv")
            
        shutil.move(main_csv, backup_path)
        print(f"📦 已將舊資料備份至: {backup_path}")

    # 3. 下載今日資料
    url = f"https://www.fhtrust.com.tw/api/assetsExcel/{target_etf}/{today_str}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    try:
        print(f"🌐 正在抓取今日 ({today_str}) 資料...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # 讀取並定位 Header
        raw_df = pd.read_excel(io.BytesIO(response.content), header=None)
        header_row = None
        for i, row in raw_df.iterrows():
            if "證券名稱" in row.values:
                header_row = i
                break
        
        if header_row is None:
            raise ValueError("在 Excel 中找不到 '證券名稱' 欄位，請檢查官網檔案格式是否更動。")

        df_today = pd.read_excel(io.BytesIO(response.content), skiprows=header_row)
        df_today = df_today.dropna(how='all')
        # 排除合計列
        df_today = df_today[~df_today.iloc[:, 0].astype(str).str.contains("合計|備註|註", na=False)]
        
        # 儲存最新的 991a.csv
        df_today.to_csv(main_csv, index=False, encoding="utf-8-sig")
        print(f"✅ 今日資料已儲存為: {main_csv}")

        # 4. 進行比對
        compare_holdings(main_csv, backup_folder, main_html)

    except Exception as e:
        print(f"❌ 執行失敗: {e}")

def clean_numeric(series):
    """清理數值欄位：移除逗號並轉為 float"""
    return pd.to_numeric(series.astype(str).str.replace(',', '').replace('nan', '0'), errors='coerce').fillna(0)

def compare_holdings(current_file, backup_folder, output_html):
    # 取得最新的一個備份檔
    list_of_files = glob.glob(f'{backup_folder}/*.csv')
    if not list_of_files:
        print("⚠️ 尚無歷史備份資料，僅產生基本 HTML。")
        df = pd.read_csv(current_file)
        df.to_html(output_html, index=False)
        return

    latest_backup = max(list_of_files, key=os.path.getctime)
    print(f"🔍 正在與昨日資料比對: {latest_backup}")

    df_new = pd.read_csv(current_file)
    df_old = pd.read_csv(latest_backup)

    # 識別關鍵欄位 (復華的欄位名稱通常是 '證券代號' 或 '證券名稱'，數量欄位通常是 '持股股數')
    key_col = "證券代號" if "證券代號" in df_new.columns else "證券名稱"
    qty_col = "持股股數" if "持股股數" in df_new.columns else df_new.columns[2]

    # --- 關鍵修正：確保數量欄位是數字 ---
    df_new[qty_col] = clean_numeric(df_new[qty_col])
    df_old[qty_col] = clean_numeric(df_old[qty_col])

    # 合併新舊資料進行比對
    merged = pd.merge(
        df_new[[key_col, '證券名稱', qty_col]], 
        df_old[[key_col, qty_col]], 
        on=key_col, how='outer', suffixes=('_新', '_舊')
    )

    # 處理 NaN 值 (若新資料沒該股 = 被賣掉；若舊資料沒該股 = 新買進)
    merged[f'{qty_col}_新'] = merged[f'{qty_col}_新'].fillna(0)
    merged[f'{qty_col}_舊'] = merged[f'{qty_col}_舊'].fillna(0)

    # 再次確認證券名稱 (避免合併後出現 NaN)
    if '證券名稱_x' in merged.columns: # 如果 merge 產生了兩個名稱
        merged['證券名稱'] = merged['證券名稱_新'].fillna(merged['證券名稱_舊'])

    def detect_change(row):
        new_v = row[f'{qty_col}_新']
        old_v = row[f'{qty_col}_舊']
        
        if old_v == 0 and new_v > 0: return "🆕 第一次買進"
        if new_v == 0 and old_v > 0: return "🚫 全部賣出"
        
        diff = new_v - old_v
        if diff > 0: return f"🔺 增加持股 ({int(diff):+,})"
        if diff < 0: return f"🔻 減少持股 ({int(diff):+,})"
        return "━ 持股不變"

    merged['異動狀態'] = merged.apply(detect_change, axis=1)
    
    # 整理輸出表格 (選取重要欄位)
    final_df = merged[[key_col, '證券名稱', f'{qty_col}_舊', f'{qty_col}_新', '異動狀態']]
    final_df.columns = ['代號', '名稱', '昨日股數', '今日股數', '異動狀態']

    # 製作 HTML 樣式
    html_style = """
    <style>
        body { font-family: "Microsoft JhengHei", sans-serif; margin: 20px; }
        table { border-collapse: collapse; width: 100%; max-width: 1000px; }
        th { background-color: #f2f2f2; position: sticky; top: 0; }
        td, th { border: 1px solid #ddd; padding: 10px; text-align: left; }
        tr:hover { background-color: #f5f5f5; }
        .status-new { color: #0066cc; font-weight: bold; }
        .status-up { color: #d9534f; font-weight: bold; } /* 紅色 */
        .status-down { color: #5cb85c; font-weight: bold; } /* 綠色 */
        .status-sold { color: #777; text-decoration: line-through; background-color: #eee; }
    </style>
    """
    
    html_body = final_df.to_html(index=False, escape=False)
    
    # 根據狀態套用 CSS Class
    html_body = html_body.replace('🆕 第一次買進', '<span class="status-new">🆕 第一次買進</span>')
    html_body = html_body.replace('🔺 增加', '<span class="status-up">🔺 增加')
    html_body = html_body.replace('🔻 減少', '<span class="status-down">🔻 減少')
    html_body = html_body.replace('🚫 全部賣出', '<span class="status-sold">🚫 全部賣出</span>')

    with open(output_html, "w", encoding="utf-8") as f:
        f.write(f"<html><head><meta charset='utf-8'>{html_style}</head><body>")
        f.write(f"<h1>ETF 每日持股異動報告 ({datetime.now().strftime('%Y-%m-%d')})</h1>")
        f.write(f"<p>比對基準檔案: {os.path.basename(latest_backup)}</p>")
        f.write(html_body)
        f.write("</body></html>")
    
    print(f"✨ 網頁報告已產生: {output_html}")

if __name__ == "__main__":
    run_daily_update()
