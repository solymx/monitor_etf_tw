import requests
from bs4 import BeautifulSoup
import json
import html
import pandas as pd
from datetime import datetime
import os

# --- 設定區 ---
target_url = "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW" # 統一 FANG+
csv_filename = "981a.csv"   # <--- 這裡改成你要的名字
html_filename = "981a.html"
archive_dir = "981a"        # 歷史存檔資料夾

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def generate_html(changes, current_df, update_time):
    # 準備表格內容
    table_rows = ""
    try:
        current_df = current_df.sort_values(by='權重(%)', ascending=False)
    except:
        pass

    for index, row in current_df.iterrows():
        share_str = f"{int(row['股數']):,}"
        weight_str = f"{row['權重(%)']}%"
        table_rows += f"""
        <tr>
            <td><span class="code-badge">{row['股票代號']}</span> {row['股票名稱']}</td>
            <td class="text-right">{share_str}</td>
            <td class="text-right">{weight_str}</td>
        </tr>
        """
        
    html_content = f"""
    <!DOCTYPE html>
    <html lang="zh-TW">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>ETF 持股監控報告</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; background-color: #f8f9fa; color: #333; margin: 0; padding: 20px; }}
            .container {{ max_width: 800px; margin: 0 auto; background: #fff; padding: 20px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
            h1 {{ text-align: center; color: #2c3e50; font-size: 22px; margin-bottom: 5px; }}
            .date {{ text-align: center; color: #7f8c8d; font-size: 13px; margin-bottom: 30px; }}
            h2 {{ font-size: 18px; border-left: 5px solid #3498db; padding-left: 10px; margin-top: 30px; margin-bottom: 15px; color: #2c3e50; }}
            .card {{ border: 1px solid #eee; border-radius: 8px; padding: 12px 15px; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center; background: #fff; }}
            .badge {{ padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; color: #fff; min-width: 50px; text-align: center; }}
            .bg-new {{ border-left: 4px solid #e74c3c; }} 
            .bg-exit {{ border-left: 4px solid #2ecc71; }} 
            .badge-new {{ background-color: #e74c3c; }}
            .badge-up {{ background-color: #e67e22; }}
            .badge-exit {{ background-color: #27ae60; }}
            .badge-down {{ background-color: #2ecc71; }}
            .stock-info {{ display: flex; flex-direction: column; }}
            .stock-name {{ font-weight: 600; font-size: 16px; }}
            .stock-code {{ font-size: 12px; color: #999; }}
            .change-msg {{ font-size: 13px; font-weight: 500; text-align: right; margin-top: 4px; }}
            .empty-msg {{ text-align: center; color: #bbb; padding: 15px; font-style: italic; background: #f9f9f9; border-radius: 5px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
            th, td {{ padding: 12px 8px; border-bottom: 1px solid #eee; font-size: 14px; }}
            th {{ background-color: #f8f9fa; color: #666; font-weight: 600; text-align: left; }}
            tr:last-child td {{ border-bottom: none; }}
            .text-right {{ text-align: right; font-family: 'SF Mono', Consolas, 'Courier New', monospace; }}
            .code-badge {{ background: #eee; color: #555; padding: 2px 6px; border-radius: 4px; font-size: 12px; margin-right: 5px; }}
            footer {{ margin-top: 40px; text-align: center; font-size: 12px; color: #ccc; border-top: 1px solid #eee; padding-top: 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 ETF 持股監控日報</h1>
            <div class="date">更新時間: {update_time}</div>
            <h2>🔥 今日持股變動</h2>
            <div id="changes-list">
    """

    if not changes:
        html_content += '<div class="empty-msg">今日持股無任何變動 (或無舊資料可比對)</div>'
    else:
        for item in changes:
            card_class = "bg-new" if item['type'] in ['new', 'up'] else "bg-exit"
            msg_color = '#c0392b' if item['type'] in ['new', 'up'] else '#27ae60'
            badge_map = {'new': ('badge-new', '建倉'), 'exit': ('badge-exit', '清倉'), 'up': ('badge-up', '加碼'), 'down': ('badge-down', '減碼')}
            badge_class, badge_text = badge_map.get(item['type'], ('', ''))
            
            html_content += f"""
            <div class="card {card_class}">
                <div class="stock-info"><span class="stock-name">{item['name']}</span><span class="stock-code">{item['code']}</span></div>
                <div style="text-align: right;"><span class="badge {badge_class}">{badge_text}</span><div class="change-msg" style="color: {msg_color}">{item['msg']}</div></div>
            </div>
            """

    html_content += f"""
            </div>
            <h2>📋 當前完整持股 ({len(current_df)} 檔)</h2>
            <table>
                <thead><tr><th>股票名稱</th><th class="text-right">持有股數</th><th class="text-right">權重</th></tr></thead>
                <tbody>{table_rows}</tbody>
            </table>
            <footer>Generated by GitHub Actions | Source: ezmoney</footer>
        </div>
    </body>
    </html>
    """
    with open(html_filename, "w", encoding="utf-8") as f:
        f.write(html_content)

def compare_holdings(new_df, old_df):
    changes = []
    new_dict = new_df.set_index('股票代號')['股數'].to_dict()
    if old_df is not None:
        old_dict = old_df.set_index('股票代號')['股數'].to_dict()
        old_keys = set(old_dict.keys())
        new_keys = set(new_dict.keys())
        for code in (new_keys - old_keys):
            name = new_df[new_df['股票代號'] == code]['股票名稱'].values[0]
            changes.append({'type': 'new', 'code': code, 'name': name, 'msg': f"買進 {new_dict[code]:,.0f} 股"})
        for code in (old_keys - new_keys):
            name = old_df[old_df['股票代號'] == code]['股票名稱'].values[0]
            changes.append({'type': 'exit', 'code': code, 'name': name, 'msg': "全數賣出"})
        for code in (old_keys & new_keys):
            diff = new_dict[code] - old_dict[code]
            name = new_df[new_df['股票代號'] == code]['股票名稱'].values[0]
            if diff > 0: changes.append({'type': 'up', 'code': code, 'name': name, 'msg': f"+{diff:,.0f} 股"})
            elif diff < 0: changes.append({'type': 'down', 'code': code, 'name': name, 'msg': f"-{abs(diff):,.0f} 股"})
    sort_order = {'new': 0, 'up': 1, 'down': 2, 'exit': 3}
    changes.sort(key=lambda x: sort_order.get(x['type'], 99))
    return changes

def get_etf_holdings():
    try:
        response = requests.get(target_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        data_div = soup.find("div", id="DataAsset")
        
        if not data_div: return

        raw_json = data_div.get("data-content")
        data = json.loads(html.unescape(raw_json))
        
        stock_data = None
        for item in data:
            if item.get("AssetCode") == "ST":
                stock_data = item.get("Details")
                break
        
        if stock_data:
            df_new = pd.DataFrame(stock_data)[['DetailCode', 'DetailName', 'Share', 'NavRate']]
            df_new.columns = ['股票代號', '股票名稱', '股數', '權重(%)']
            df_new['股數'] = pd.to_numeric(df_new['股數'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            df_new['權重(%)'] = pd.to_numeric(df_new['權重(%)'], errors='coerce').fillna(0)

            df_old = None
            if os.path.exists(csv_filename):
                try:
                    df_old = pd.read_csv(csv_filename, dtype={'股票代號': str})
                    df_old['股數'] = pd.to_numeric(df_old['股數'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                except: pass

            changes_list = compare_holdings(df_new, df_old)
            generate_html(changes_list, df_new, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            
            # --- 存檔 ---
            # 1. 覆蓋 981a.csv (作為明日比對基準)
            df_new.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            
            # 2. 備份到 981a 資料夾 (作為歷史紀錄)
            if not os.path.exists(archive_dir):
                os.makedirs(archive_dir)
                
            today_str = datetime.now().strftime('%Y-%m-%d')
            archive_path = os.path.join(archive_dir, f"{today_str}.csv")
            
            df_new.to_csv(archive_path, index=False, encoding='utf-8-sig')
            print(f"資料已更新：{csv_filename} 與 {archive_path}")
            
        else:
            print("找不到資料")

    except Exception as e:
        print(f"錯誤: {e}")

if __name__ == "__main__":
    get_etf_holdings()
