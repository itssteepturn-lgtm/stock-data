"""
用 baostock 抓全市场股票的前复权日线数据。
baostock 是专门给量化分析用的批量数据接口（免注册），不是东财那种给个人看盘小组件用的接口，
批量查询的稳定性应该好很多。

数据深度策略：某只股票如果本地还没有数据（或者数据太浅），第一次会抓近2年的完整历史
（筹码分布这类计算需要足够长的历史才准）；已经有足够历史的，以后每天只抓最近10天补齐，
不用每天都重新下载2年数据。

覆盖范围说明：baostock 主要覆盖沪深主板/中小板/创业板/科创板，
北交所股票大概率不在里面——这部分会有缺口，先接受这个小缺口，
以后如果确实需要，再单独想办法补北交所这一小块。
"""
import baostock as bs
import json
import os
import sys
import time
from datetime import datetime, timedelta

DATA_DIR = "data/stocks"
META_FILE = "data/meta.json"
MAX_DAYS_KEPT = 1500
DEEP_SEED_DAYS = 730   # 第一次没有本地数据时，往回抓这么多天
TOPUP_DAYS = 10        # 已经有历史的，平时只补这么多天
MIN_DEPTH_TO_SKIP_SEED = 300  # 本地数据到这个天数以上，就不用再当"第一次"处理

def reconnect():
    try:
        bs.logout()
    except Exception:
        pass
    time.sleep(1)
    bs.login()

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
    names = {}
    while rs.error_code == '0' and rs.next():
        row = rs.get_row_data()
        code = row[0]
        code_name = row[2] if len(row) > 2 else ''
        if code.startswith('sh.60') or code.startswith('sh.68') or \
           code.startswith('sz.00') or code.startswith('sz.30'):
            codes.append(code)
            names[code.split('.')[1]] = code_name

    print(f"共 {len(codes)} 只股票")
    if len(codes) < 1000:
        print("数量明显不对，判定本次失败")
        bs.logout()
        sys.exit(1)

    updated, failed = 0, 0
    date_counts = {}
    RELOGIN_EVERY = 300  # 跑这么多只就主动断开重连一次，不要一个连接从头扛到尾

    for i, code in enumerate(codes):
        if i % 500 == 0:
            print(f"  进度 {i}/{len(codes)}")
        if i > 0 and i % RELOGIN_EVERY == 0:
            reconnect()

        raw_code = code.split('.')[1]
        file_path = os.path.join(DATA_DIR, f"{raw_code}.json")
        existing = []
        if os.path.exists(file_path):
            try:
                with open(file_path) as f:
                    existing = json.load(f)
            except Exception:
                existing = []

        need_deep_seed = len(existing) < MIN_DEPTH_TO_SKIP_SEED
        start_date = (datetime.now() - timedelta(days=DEEP_SEED_DAYS if need_deep_seed else TOPUP_DAYS)).strftime('%Y-%m-%d')

        bars = None
        for attempt in range(2):  # 失败重试一次（顺便处理断线）
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
                bars = [
                    {"date": r[0], "open": float(r[1]), "high": float(r[2]), "low": float(r[3]),
                     "close": float(r[4]), "vol": float(r[5]), "amount": float(r[6]),
                     "turnover": float(r[7]) if r[7] else 0}
                    for r in rows if r[4]
                ]
                break
            except Exception:
                reconnect()

        if not bars:
            failed += 1
            continue

        if need_deep_seed:
            merged = bars
        else:
            by_date = {b["date"]: b for b in existing}
            for b in bars:
                by_date[b["date"]] = b
            merged = [by_date[d] for d in sorted(by_date.keys())]

        if len(merged) > MAX_DAYS_KEPT:
            merged = merged[-MAX_DAYS_KEPT:]

        try:
            with open(file_path, 'w') as f:
                json.dump(merged, f)
            updated += 1
            if merged:
                date_counts[merged[-1]["date"]] = date_counts.get(merged[-1]["date"], 0) + 1
        except Exception:
            failed += 1

    bs.logout()

    # 写全市场代码+名称清单，网页那边全市场选股/回测要靠这份文件知道该扫哪些股票
    stock_list = [{"code": c.split('.')[1], "name": names.get(c.split('.')[1], '')} for c in codes]
    with open('data/codes.json', 'w') as f:
        json.dump(stock_list, f, ensure_ascii=False)

    last_update_date = max(date_counts, key=date_counts.get) if date_counts else None
    with open(META_FILE, 'w') as f:
        json.dump({
            "lastUpdateDate": last_update_date,
            "updated": updated, "failed": failed, "total": len(codes),
            "ranAt": datetime.utcnow().isoformat() + "Z"
        }, f)

    print(f"完成：更新{updated}只，失败{failed}只，数据日期{last_update_date}")

if __name__ == "__main__":
    main()
