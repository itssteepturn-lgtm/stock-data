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
MAX_DAYS_KEPT = 150
DEEP_SEED_DAYS = 90          # 第一次没有本地数据时，往回抓这么多天（约3个月）
TOPUP_DAYS = 10               # 已经有历史的，平时只补这么多天
MIN_DEPTH_TO_SKIP_SEED = 80   # 本地数据到这个天数以上，就不用再当"第一次"处理（必须小于存储上限150，否则永远没法"毕业"到省事的补量模式）
CHECKPOINT_EVERY = 100        # 跑这么多只就提交推送一次
RECONNECT_SECONDS = 180       # 连接活了这么久（不管跑了几只）就强制重连一次
TIME_BUDGET_SECONDS = 12 * 60  # 单次运行最多跑这么久（12分钟），配合每15分钟一次的定时，跑很多次短的，比跑一次超长的更稳

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

def write_meta(total, date_counts, note=''):
    # 真实总数：直接数文件夹里的文件，且区分"确认正常"(>=2天数据)和"刚起步、还在攒"(只有1天)——
    # 不是心里记"这次抓了几个"，那样每次运行的局部计数会互相覆盖，显示出"数字倒退"的假象
    confirmed, accumulating = 0, 0
    try:
        for fname in os.listdir(DATA_DIR):
            if not fname.endswith('.json'):
                continue
            try:
                with open(os.path.join(DATA_DIR, fname)) as f:
                    n = len(json.load(f))
                if n >= 2:
                    confirmed += 1
                elif n == 1:
                    accumulating += 1
            except Exception:
                pass
    except Exception:
        pass
    last_update_date = max(date_counts, key=date_counts.get) if date_counts else None
    with open(META_FILE, 'w') as f:
        json.dump({
            "lastUpdateDate": last_update_date,
            "updated": confirmed, "accumulating": accumulating, "total": total,
            "ranAt": datetime.utcnow().isoformat() + "Z",
            "note": note
        }, f)

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    run_start = time.time()

    lg = None
    for attempt in range(3):
        lg = bs.login()
        if lg.error_code == '0':
            break
        print(f"登录失败（第{attempt+1}次）: {lg.error_msg}，等10秒重试")
        time.sleep(10)
    if not lg or lg.error_code != '0':
        print("登录彻底失败，本次先放弃，下次自动运行再试")
        sys.exit(1)
    last_login_time = time.time()

    today = datetime.now().strftime('%Y-%m-%d')
    codes = []
    names = {}

    # 每次都现拉最新清单（今天/昨天兜底），这样新股会自动被发现，不用手动维护清单
    query_days = [(datetime.now()-timedelta(days=n)).strftime('%Y-%m-%d') for n in range(0,6)]
    for query_day in query_days:
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

    # 现拉都失败的话，退回去用仓库里上次存的清单顶着（新股会晚几天才发现，但不会整体失败）
    if not codes and os.path.exists('data/codes.json'):
        try:
            with open('data/codes.json') as f:
                cached = json.load(f)
            if isinstance(cached, list) and len(cached) > 1000:
                print(f"现拉清单失败，改用仓库里缓存的清单（{len(cached)}只）")
                for item in cached:
                    raw = item.get('code') if isinstance(item, dict) else None
                    if raw:
                        prefix = 'sh.' if raw.startswith('6') else 'sz.'
                        codes.append(prefix + raw)
                        names[raw] = item.get('name', '')
        except Exception as e:
            print("读取本地清单缓存失败:", e)

    print(f"共 {len(codes)} 只股票")

    # 顺便抓主要指数（存到单独目录，避免代码跟个股撞号，比如 000001 既是上证指数又是平安银行）
    INDEX_DIR = "data/indices"
    os.makedirs(INDEX_DIR, exist_ok=True)
    INDEXES = [
        ("sh.000001", "sh000001"), ("sz.399001", "sz399001"),
        ("sz.399006", "sz399006"), ("sh.000688", "sh000688"),
    ]
    for bcode, filename in INDEXES:
        try:
            rs3 = bs.query_history_k_data_plus(
                bcode, "date,open,high,low,close,volume,amount",
                start_date=(datetime.now()-timedelta(days=DEEP_SEED_DAYS)).strftime('%Y-%m-%d'),
                end_date=today, frequency="d", adjustflag="2"
            )
            rows = []
            while rs3.error_code == '0' and rs3.next():
                rows.append(rs3.get_row_data())
            bars = [{"date":r[0],"open":float(r[1]),"high":float(r[2]),"low":float(r[3]),
                     "close":float(r[4]),"vol":float(r[5]),"amount":float(r[6])} for r in rows if r[4]]
            if bars:
                with open(os.path.join(INDEX_DIR, f"{filename}.json"), 'w') as f:
                    json.dump(bars, f)
        except Exception as e:
            print(f"指数{bcode}抓取失败: {e}")

    if len(codes) < 1000:
        print("数量明显不对，判定本次失败")
        bs.logout()
        sys.exit(1)

    # 先把清单存一份（就算后面被时间预算截断，清单和名字也不会丢）
    stock_list = [{"code": c.split('.')[1], "name": names.get(c.split('.')[1], '')} for c in codes]
    with open('data/codes.json', 'w') as f:
        json.dump(stock_list, f, ensure_ascii=False)

    CURSOR_FILE = 'data/collect-cursor.json'
    def load_cursor(total):
        if os.path.exists(CURSOR_FILE):
            try:
                with open(CURSOR_FILE) as f:
                    idx = json.load(f).get('nextIndex', 0)
                if isinstance(idx, int) and 0 <= idx < total:
                    return idx
            except Exception:
                pass
        return 0
    def save_cursor(idx):
        with open(CURSOR_FILE, 'w') as f:
            json.dump({'nextIndex': idx}, f)

    updated, failed = 0, 0
    failed_codes = []
    date_counts = {}
    stopped_early = False
    total = len(codes)
    start_idx = load_cursor(total)
    print(f"从第 {start_idx} 个接着抓（上次停在这，不是每次都从头开始）")

    processed = 0
    for offset in range(total):
        i = (start_idx + offset) % total
        processed = offset
        code = codes[i]
        if processed % 500 == 0:
            print(f"  进度 {processed}/{total}（当前位置{i}）")

        if time.time() - run_start > TIME_BUDGET_SECONDS:
            print(f"到时间预算了，先收尾（这次处理了{processed}只，下次从第{i}个接着来）")
            stopped_early = True
            save_cursor(i)
            break

        if time.time() - last_login_time > RECONNECT_SECONDS:
            reconnect()
            last_login_time = time.time()

        if processed > 0 and processed % CHECKPOINT_EVERY == 0:
            write_meta(total, date_counts, note='采集进行中')
            save_cursor(i)
            git_checkpoint(f'{processed}/{total} (位置{i})')

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
        date_ranges_to_try = [start_date]
        if need_deep_seed:
            # 深度种子如果一直失败，最后退而求其次，只求拿到最近几天，先证明这只股票是正常的
            date_ranges_to_try.append((datetime.now() - timedelta(days=TOPUP_DAYS)).strftime('%Y-%m-%d'))
        for try_start in date_ranges_to_try:
            for attempt in range(2):
                try:
                    rs2 = bs.query_history_k_data_plus(
                        code,
                        "date,open,high,low,close,volume,amount,turn",
                        start_date=try_start, end_date=today,
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
            if bars:
                break

        if not bars:
            failed += 1
            failed_codes.append(raw_code)
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
            failed_codes.append(raw_code)

    try:
        bs.logout()
    except Exception:
        pass

    if not stopped_early:
        save_cursor(0)  # 一整圈都跑完了，下次从头开始新一轮

    note = '本轮因时间预算提前收尾，还有未处理的股票，下次运行会继续' if stopped_early else '本轮全部处理完成，下次开始新一轮'
    write_meta(total, date_counts, note=note)
    with open('data/failed-codes.json', 'w') as f:
        json.dump(sorted(set(failed_codes)), f)

    git_checkpoint('final')

    print(f"完成：更新{updated}只，失败{failed}只，{note}")

if __name__ == "__main__":
    main()
