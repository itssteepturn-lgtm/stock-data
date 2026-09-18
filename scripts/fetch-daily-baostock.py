"""
用 baostock 抓全市场股票的前复权日线数据。
baostock 是专门给量化分析用的批量数据接口（免注册），不是东财那种给个人看盘小组件用的接口，
批量查询的稳定性应该好很多。

数据深度策略：某只股票如果本地还没有数据（或者数据太浅），第一次会抓约1.5年历史
（筹码分布这类计算需要足够长的历史才准）；已经有足够历史的，以后每天只抓最近10天补齐，
不用每天都重新下载。

可靠性设计：
- 每跑一批（CHECKPOINT_EVERY只）就直接提交推送一次，不等全部跑完才存——
  就算中途被打断（比如GitHub 6小时上限），已经跑完的部分也保得住，不会白跑
- 连接每隔几分钟强制重连一次（不只是按数量），因为长时间不断开连接，
  底层的连接本身会被断掉，跟查询快慢关系不大
- 单次运行给自己设了时间预算，快到上限就主动收尾，绝不会被硬杀到最后一刻

覆盖范围说明：baostock 主要覆盖沪深主板/中小板/创业板/科创板，
北交所股票大概率不在里面——这部分会有缺口，先接受这个小缺口，
以后如果确实需要，再单独想办法补北交所这一小块。
"""
import baostock as bs
import json
import os
import sys
import time
import subprocess
from datetime import datetime, timedelta

DATA_DIR = "data/stocks"
META_FILE = "data/meta.json"
MAX_DAYS_KEPT = 1500
DEEP_SEED_DAYS = 500          # 第一次没有本地数据时，往回抓这么多天
TOPUP_DAYS = 10               # 已经有历史的，平时只补这么多天
MIN_DEPTH_TO_SKIP_SEED = 300  # 本地数据到这个天数以上，就不用再当"第一次"处理
CHECKPOINT_EVERY = 200        # 跑这么多只就提交推送一次
RECONNECT_SECONDS = 180       # 连接活了这么久（不管跑了几只）就强制重连一次
TIME_BUDGET_SECONDS = 5 * 3600  # 单次运行最多跑这么久，到点就收尾（GitHub上限6小时，留余量）

def reconnect():
    try:
        bs.logout()
    except Exception:
        pass
    time.sleep(1)
    bs.login()

def git_checkpoint(tag):
    """把目前抓到的数据直接提交推送。失败也不影响继续抓，下次checkpoint再试一次整体提交。"""
    try:
        subprocess.run(['git', 'add', 'data/'], check=True)
        diff = subprocess.run(['git', 'diff', '--staged', '--quiet'])
        if diff.returncode == 0:
            return  # 没有变化，不用提交
        subprocess.run(['git', 'commit', '-m', f'data checkpoint {tag}'], check=True)
        subprocess.run(['git', 'fetch', 'origin', 'main'], check=True)
        subprocess.run(['git', 'merge', 'origin/main', '--no-edit', '-X', 'ours',
                         '-m', 'merge remote changes, prefer freshly collected data'], check=True)
        subprocess.run(['git', 'push'], check=True)
        print(f"  已提交检查点：{tag}")
    except subprocess.CalledProcessError as e:
        print(f"  检查点提交失败（继续抓，下次再试）: {e}")

def write_meta(updated, failed, total, date_counts, note=''):
    last_update_date = max(date_counts, key=date_counts.get) if date_counts else None
    with open(META_FILE, 'w') as f:
        json.dump({
            "lastUpdateDate": last_update_date,
            "updated": updated, "failed": failed, "total": total,
            "ranAt": datetime.utcnow().isoformat() + "Z",
            "note": note
        }, f)

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    run_start = time.time()

    lg = bs.login()
    if lg.error_code != '0':
        print("登录失败:", lg.error_msg)
        sys.exit(1)
    last_login_time = time.time()

    today = datetime.now().strftime('%Y-%m-%d')
    codes = []
    names = {}

    # 清单优先用仓库里已经存好的（stocks列表变化很小，没必要每次都问baostock）
    CODES_CACHE = 'data/codes.json'
    if os.path.exists(CODES_CACHE):
        try:
            with open(CODES_CACHE) as f:
                cached = json.load(f)
            if isinstance(cached, list) and len(cached) > 1000:
                print(f"用仓库里已有的股票清单（{len(cached)}只），不重新问baostock要清单")
                for item in cached:
                    raw = item.get('code') if isinstance(item, dict) else None
                    if not raw:
                        continue
                    prefix = 'sh.' if raw.startswith('6') else 'sz.'
                    codes.append(prefix + raw)
                    names[raw] = item.get('name', '')
        except Exception as e:
            print("读取本地清单缓存失败:", e)

    if not codes:
        for query_day in [today, (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')]:
            print(f"拉取股票清单（截至 {query_day}）...")
            rs = bs.query_all_stock(day=query_day)
            tmp_codes, tmp_names = [], {}
            while rs.error_code == '0' and rs.next():
                row = rs.get_row_data()
                code = row[0]
                code_name = row[2] if len(row) > 2 else ''
                if code.startswith('sh.60') or code.startswith('sh.68') or \
                   code.startswith('sz.00') or code.startswith('sz.30'):
                    tmp_codes.append(code)
                    tmp_names[code.split('.')[1]] = code_name
            print(f"  拿到 {len(tmp_codes)} 只")
            if len(tmp_codes) > 1000:
                codes, names = tmp_codes, tmp_names
                break

    print(f"共 {len(codes)} 只股票")
    if len(codes) < 1000:
        print("数量明显不对，判定本次失败")
        bs.logout()
        sys.exit(1)

    # 先把清单存一份（就算后面被时间预算截断，清单和名字也不会丢）
    stock_list = [{"code": c.split('.')[1], "name": names.get(c.split('.')[1], '')} for c in codes]
    with open('data/codes.json', 'w') as f:
        json.dump(stock_list, f, ensure_ascii=False)

    updated, failed = 0, 0
    date_counts = {}
    stopped_early = False

    for i, code in enumerate(codes):
        if i % 500 == 0:
            print(f"  进度 {i}/{len(codes)}")

        if time.time() - run_start > TIME_BUDGET_SECONDS:
            print(f"到时间预算了，先收尾（处理到第{i}只，剩下的下次继续）")
            stopped_early = True
            break

        if time.time() - last_login_time > RECONNECT_SECONDS:
            reconnect()
            last_login_time = time.time()

        if i > 0 and i % CHECKPOINT_EVERY == 0:
            write_meta(updated, failed, len(codes), date_counts, note='采集进行中')
            git_checkpoint(f'{i}/{len(codes)}')

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
        for attempt in range(2):
            try:
                rs2 = bs.query_history_k_data_plus(
                    code,
                    "date,open,high,low,close,volume,amount,turn",
                    start_date=start_date, end_date=today,
                    frequency="d", adjustflag="2"
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
                last_login_time = time.time()

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

    try:
        bs.logout()
    except Exception:
        pass

    note = '本轮因时间预算提前收尾，还有未处理的股票，下次运行会继续' if stopped_early else '本轮全部处理完成'
    write_meta(updated, failed, len(codes), date_counts, note=note)
    git_checkpoint('final')

    print(f"完成：更新{updated}只，失败{failed}只，{note}")

if __name__ == "__main__":
    main()
