"""
用 baostock 抓全市场股票的最新前复权日线数据。
baostock 是专门给量化分析用的批量数据接口（免注册），不是东财那种给个人看盘小组件用的接口，
批量查询的稳定性应该好很多。

覆盖范围说明：baostock 主要覆盖沪深主板/中小板/创业板/科创板，
北交所股票大概率不在里面——这部分会有缺口，先接受这个小缺口，
以后如果确实需要，再单独想办法补北交所这一小块。
"""
import baostock as bs
import json
import os
import sys
from datetime import datetime, timedelta

DATA_DIR = "data/stocks"
META_FILE = "data/meta.json"
MAX_DAYS_KEPT = 1500

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    lg = bs.login()
    if lg.error_code != '0':
        print("登录失败:", lg.error_msg)
        sys.exit(1)

    today = datetime.now().strftime('%Y-%m-%d')
    print(f"拉取股票清单（截至 {today}）...")
    rs = bs.query_all_stock(day=today)
    codes = []
    while rs.error_code == '0' and rs.next():
        row = rs.get_row_data()
        code = row[0]
        # 只要沪深主板/中小板/创业板/科创板股票，排除指数和其它品种
        if code.startswith('sh.60') or code.startswith('sh.68') or \
           code.startswith('sz.00') or code.startswith('sz.30'):
            codes.append(code)

    print(f"共 {len(codes)} 只股票")
    if len(codes) < 1000:
        print("数量明显不对，判定本次失败")
        bs.logout()
        sys.exit(1)

    start_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    updated, failed = 0, 0
    date_counts = {}

    for i, code in enumerate(codes):
        if i % 500 == 0:
            print(f"  进度 {i}/{len(codes)}")
        try:
            rs2 = bs.query_history_k_data_plus(
                code,
                "date,open,high,low,close,volume,amount,turn",
                start_date=start_date, end_date=today,
                frequency="d", adjustflag="2"  # 前复权，跟网页里其它地方口径一致
            )
            rows = []
            while rs2.error_code == '0' and rs2.next():
                rows.append(rs2.get_row_data())
            if not rows:
                failed += 1
                continue
            last = rows[-1]
            if not last[4]:  # close 为空，多半停牌
                failed += 1
                continue
            bar = {
                "date": last[0], "open": float(last[1]), "high": float(last[2]),
                "low": float(last[3]), "close": float(last[4]),
                "vol": float(last[5]), "amount": float(last[6]),
                "turnover": float(last[7]) if last[7] else 0
            }
            raw_code = code.split('.')[1]
            file_path = os.path.join(DATA_DIR, f"{raw_code}.json")
            arr = []
            if os.path.exists(file_path):
                try:
                    with open(file_path) as f:
                        arr = json.load(f)
                except Exception:
                    arr = []
            if arr and arr[-1]["date"] == bar["date"]:
                arr[-1] = bar
            else:
                arr.append(bar)
            if len(arr) > MAX_DAYS_KEPT:
                arr = arr[-MAX_DAYS_KEPT:]
            with open(file_path, 'w') as f:
                json.dump(arr, f)
            updated += 1
            date_counts[bar["date"]] = date_counts.get(bar["date"], 0) + 1
        except Exception as e:
            failed += 1

    bs.logout()

    last_update_date = max(date_counts, key=date_counts.get) if date_counts else None
    with open(META_FILE, 'w') as f:
        json.dump({
            "lastUpdateDate": last_update_date,
            "updated": updated, "failed": failed, "total": len(codes),
            "ranAt": datetime.utcnow().isoformat()
        }, f)

    print(f"完成：更新{updated}只，失败{failed}只，数据日期{last_update_date}")

if __name__ == "__main__":
    main()
