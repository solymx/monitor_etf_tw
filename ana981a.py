import pandas as pd
import os
import glob
import json
from datetime import datetime

def analyze_etf_holdings(csv_folder_path, output_html="ana981a.html"):
    """
    讀取資料夾內所有 CSV 檔案，分析持股趨勢並生成互動式 HTML 報告。
    """
    if not os.path.exists(csv_folder_path):
        print(f"錯誤：找不到資料夾 {csv_folder_path}")
        return

    # 1. 取得所有 CSV 檔案路徑
    file_pattern = os.path.join(csv_folder_path, "*.csv")
    files = glob.glob(file_pattern)
    
    if not files:
        print(f"在 {csv_folder_path} 路徑下找不到任何 CSV 檔案。")
        return

    all_data = []

    # 2. 讀取並整合數據
    for file_path in files:
        file_name = os.path.basename(file_path)
        try:
            # 解析檔名日期 (YYYY-MM-DD.csv)
            date_str = file_name.split('.')[0]
            report_date = pd.to_datetime(date_str)
        except Exception:
            print(f"跳過無效格式檔案: {file_name}")
            continue
            
        try:
            df = pd.read_csv(file_path)
            df['日期'] = report_date
            # 清理欄位名稱與資料內容
            df.columns = [c.strip() for c in df.columns]
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str).str.strip()
            all_data.append(df)
        except Exception as e:
            print(f"讀取檔案 {file_name} 失敗: {e}")

    if not all_data:
        return

    # 整合數據，確保按股票代號與日期排序
    full_df = pd.concat(all_data).sort_values(['股票代號', '日期'])

    # 3. 計算實際買入動作 (股數增加)
    full_df['股數變化'] = full_df.groupby('股票代號')['股數'].diff()
    # 定義買入：股數增加 或 首次出現且股數大於零
    full_df['是否買入'] = (full_df['股數變化'] > 0) | (full_df['股數變化'].isna() & (full_df['股數'] > 0))

    # 計算首次建倉日
    first_appearance = full_df.groupby('股票代號')['日期'].min().reset_index()
    first_appearance.rename(columns={'日期': '首次買入日期'}, inplace=True)

    # 4. 取得日期節點 (最新 vs 10天前)
    available_dates = sorted(full_df['日期'].unique())
    latest_date = pd.Timestamp(available_dates[-1])
    target_past_date = latest_date - pd.Timedelta(days=10)
    past_date = pd.Timestamp(min(available_dates, key=lambda d: abs(pd.Timestamp(d) - target_past_date)))
    days_diff = (latest_date - past_date).days

    # 5. 重點變動分析 (Top 10)
    df_latest = full_df[full_df['日期'] == latest_date][['股票代號', '股票名稱', '股數', '權重(%)']]
    df_past = full_df[full_df['日期'] == past_date][['股票代號', '權重(%)', '股數']]
    
    comparison = pd.merge(df_latest, df_past, on='股票代號', how='outer', suffixes=('_新', '_舊'))
    comparison.fillna(0, inplace=True)
    comparison['權重變動'] = comparison['權重(%)_新'] - comparison['權重(%)_舊']

    # 彙整區間內實際加碼日期
    recent_buys_mask = (full_df['日期'] >= past_date) & (full_df['是否買入'] == True)
    recent_buys_df = full_df[recent_buys_mask].copy()
    recent_buys_df['日期字串'] = recent_buys_df['日期'].dt.strftime('%m/%d')
    
    buy_date_summary = recent_buys_df.groupby('股票代號')['日期字串'].apply(lambda x: ', '.join(sorted(list(set(x))))).reset_index()
    buy_date_summary.rename(columns={'日期字串': '實際買入日期(加碼)'}, inplace=True)
    
    comparison = pd.merge(comparison, buy_date_summary, on='股票代號', how='left')
    comparison['實際買入日期(加碼)'].fillna('無變動', inplace=True)
    
    top_increase = comparison[comparison['權重變動'] > 0].sort_values('權重變動', ascending=False).head(10)
    top_decrease = comparison[comparison['權重變動'] < 0].sort_values('權重變動', ascending=True).head(10)

    # 6. 整理最新持股名單與趨勢 JSON
    latest_holdings = pd.merge(df_latest, first_appearance, on='股票代號', how='left')
    latest_holdings = latest_holdings.sort_values(['首次買入日期', '權重(%)'], ascending=[False, False])
    
    current_stock_names = latest_holdings['股票名稱'].unique()
    trend_dict = {}
    for name in current_stock_names:
        stock_data = full_df[full_df['股票名稱'] == name].sort_values('日期')
        trend_dict[name] = {
            'dates': stock_data['日期'].dt.strftime('%Y-%m-%d').tolist(),
            'weights': stock_data['權重(%)'].tolist(),
            'shares': stock_data['股數'].tolist()
        }

    # 7. HTML 片段生成
    def df_to_html_table(df, show_buy_date=False):
        if df.empty: return "<p>期間無顯著變動</p>"
        styled_df = df.copy()
        if '權重變動' in styled_df.columns:
            styled_df['權重變動'] = styled_df['權重變動'].map(lambda x: f"{x:+.2f}%")
        return styled_df.to_html(classes='display_table', index=False, border=0)

    # 預先處理 HTML 組件以避免 f-string 解析問題
    table_inc_html = df_to_html_table(top_increase[['股票代號', '股票名稱', '權重變動', '權重(%)_新', '實際買入日期(加碼)']])
    table_dec_html = df_to_html_table(top_decrease[['股票代號', '股票名稱', '權重變動', '權重(%)_新']])
    stock_tags_html = "".join([f'<div class="stock-tag" onclick=\'showTrend("{n}", this)\'>{n}</div>' for n in current_stock_names])
    
    latest_holdings_display = latest_holdings.copy()
    latest_holdings_display['首次買入日期'] = latest_holdings_display['首次買入日期'].dt.strftime('%Y-%m-%d')
    full_table_html = latest_holdings_display[['股票代號', '股票名稱', '股數', '權重(%)', '首次買入日期']].to_html(classes='display_table', index=False, border=0)

    # 8. HTML 最終模板
    html_content = f"""
    <!DOCTYPE html>
    <html lang="zh-Hant">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>00981A ETF 持股分析</title>
        <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
        <style>
            body {{ font-family: "Microsoft JhengHei", sans-serif; margin: 20px; background-color: #f0f2f5; color: #333; }}
            .container {{ max-width: 1200px; margin: auto; background: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }}
            h1, h2, h3 {{ color: #1a73e8; text-align: center; }}
            .info-bar {{ margin-bottom: 20px; padding: 15px; background: #2c3e50; color: white; border-radius: 8px; display: flex; justify-content: space-around; flex-wrap: wrap; gap: 10px; font-weight: bold; }}
            .summary-grid {{ display: grid; grid-template-columns: 1fr; gap: 20px; margin-bottom: 30px; }}
            @media (min-width: 900px) {{ .summary-grid {{ grid-template-columns: 1fr 1fr; }} }}
            .summary-box {{ padding: 15px; border-radius: 8px; color: white; }}
            .inc-box {{ background-color: #27ae60; }}
            .dec-box {{ background-color: #e74c3c; }}
            .display_table {{ width: 100%; border-collapse: collapse; font-size: 0.85em; background: white; color: #333; border-radius: 4px; overflow: hidden; }}
            .display_table th, .display_table td {{ padding: 10px; border: 1px solid #eee; text-align: left; }}
            .display_table th {{ background-color: #f8f9fa; color: #5f6368; }}
            .stock-selector {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 20px 0; padding: 15px; background: #f8f9fa; border-radius: 8px; border: 1px solid #ddd; }}
            .stock-tag {{ padding: 5px 12px; background: white; border: 1px solid #ddd; border-radius: 20px; cursor: pointer; font-size: 0.85em; transition: 0.2s; }}
            .stock-tag:hover {{ background: #1a73e8; color: white; border-color: #1a73e8; }}
            .stock-tag.active {{ background: #1a73e8; color: white; font-weight: bold; }}
            .chart-wrapper {{ margin-top: 20px; display: none; border: 1px solid #eee; border-radius: 8px; padding: 15px; background: #fff; }}
            .scroll-table {{ max-height: 450px; overflow-y: auto; border: 1px solid #ddd; border-radius: 4px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>00981A ETF 持股異動報告</h1>
            <div class="info-bar">
                <div>最新日期：{latest_date.strftime('%Y-%m-%d')}</div>
                <div>分析區間：{days_diff} 天</div>
                <div>總持股數：{len(latest_holdings)} 支</div>
            </div>
            <div class="summary-grid">
                <div class="summary-box inc-box"><h3>📈 重點加碼 (Top 10)</h3>{table_inc_html}</div>
                <div class="summary-box dec-box"><h3>📉 重點減碼 (Top 10)</h3>{table_dec_html}</div>
            </div>
            <div class="chart-section">
                <h2>📊 單股歷史趨勢</h2>
                <p style="text-align:center; color:#666;">點擊下方名稱查看歷史變化</p>
                <div class="stock-selector">{stock_tags_html}</div>
                <div id="chartPlaceholder" style="text-align:center; padding:50px; color:#999; border:2px dashed #ddd; border-radius:8px;">請點擊股票名稱</div>
                <div id="chartWrapper" class="chart-wrapper">
                    <h3 id="selectedStockTitle" style="margin-top:0;"></h3>
                    <div id="weightChart"></div>
                    <div id="sharesChart" style="margin-top:20px;"></div>
                </div>
            </div>
            <div style="margin-top:40px;">
                <h2>📋 持有清單 (依買入日期排序)</h2>
                <div class="scroll-table">{full_table_html}</div>
            </div>
        </div>
        <script>
            const trendData = {json.dumps(trend_dict)};
            function showTrend(name, element) {{
                document.querySelectorAll('.stock-tag').forEach(el => el.classList.remove('active'));
                element.classList.add('active');
                document.getElementById('chartPlaceholder').style.display = 'none';
                document.getElementById('chartWrapper').style.display = 'block';
                document.getElementById('selectedStockTitle').innerText = name + ' 歷史走勢';
                const d = trendData[name];
                const layout = (t) => ({{ title: t, hovermode: 'x unified', margin: {{t:40, b:40, l:60, r:20}} }});
                Plotly.newPlot('weightChart', [{{x:d.dates, y:d.weights, mode:'lines+markers', name:'權重', line:{{color:'#27ae60', width:3}}}}], layout('權重趨勢 (%)'));
                Plotly.newPlot('sharesChart', [{{x:d.dates, y:d.shares, mode:'lines+markers', name:'股數', line:{{color:'#2980b9', width:3}}}}], layout('股數趨勢'));
                document.getElementById('chartWrapper').scrollIntoView({{ behavior: 'smooth', block: 'nearest' }});
            }}
        </script>
    </body>
    </html>
    """

    with open(output_html, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"成功生成報告：{output_html}")

if __name__ == "__main__":
    # 自動搜尋 981a 資料夾，若無則搜尋目前路徑
    data_path = "981a" if os.path.exists("981a") else "."
    analyze_etf_holdings(data_path)
