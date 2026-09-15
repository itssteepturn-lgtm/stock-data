// 每日收盘后跑一次：对全市场股票各自抓一条最新的"前复权"日线数据，
// 追加到对应的历史文件里。
//
// 这个脚本建议放在一个【公开】的数据仓库里跑（股票行情数据本身不是隐私信息），
// 公开仓库的GitHub Actions分钟数不限量，全市场规模每天跑几十分钟没有额度顾虑。
// 网页本身可以继续放在你的私密+Cloudflare Access仓库里，
// 通过 raw.githubusercontent.com 读这个公开数据仓库里的文件即可（跨仓库没问题）。
//
// 测试建议：先用 FETCH_LIMIT 环境变量跑一个小范围（比如50只），
// 确认GitHub的服务器能顺利访问这些接口、不会被限流，再放开到全市场。
import fs from 'fs';
import path from 'path';

const DATA_DIR = 'data/stocks';
const MAX_DAYS_KEPT = 1500;
const CONCURRENCY = 8;       // 并发别开太大，避免短时间内触发限流
const DELAY_MS = 150;        // 每个请求之间留点间隔

// GitHub服务器发请求默认不带浏览器那种请求头，容易被当成明显的爬虫流量拦截
// （表现为安静地返回空数据，不是报错），所以显式伪装成浏览器
const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Referer': 'https://quote.eastmoney.com/',
  'Accept': 'application/json, text/plain, */*'
};

async function fetchStockList(){
  const all = [];
  let pn = 1;
  const pz = 5000;
  // 沪深主板/中小板/创业板/科创板/北交所 股票（不含指数、不含B股/退市股）
  const fs_filter = 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048';
  const fields = 'f12,f13'; // 代码, 市场(0=深 1=沪)
  while(true){
    const url = `https://push2.eastmoney.com/api/qt/clist/get?pn=${pn}&pz=${pz}&po=1&np=1`+
      `&fltt=2&invt=2&fid=f3&fs=${encodeURIComponent(fs_filter)}&fields=${fields}`;
    const res = await fetch(url, { headers: HEADERS });
    if(!res.ok) throw new Error('http '+res.status);
    const json = await res.json();
    const list = (json && json.data && json.data.diff) || [];
    console.log(`  第${pn}页拿到 ${list.length} 条`);
    all.push(...list);
    if(list.length < pz) break;
    pn++;
    if(pn > 5) break; // 安全上限
  }
  return all.map(q=>({ code:q.f12, market:q.f13 })).filter(x=>x.code && x.market!=null);
}

async function fetchLatestBar(secid){
  const url = `https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=${secid}`+
    `&fields1=f1,f2,f3,f4,f5&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61`+
    `&klt=101&fqt=1&end=20500101&lmt=2`;
  const res = await fetch(url, { headers: HEADERS });
  if(!res.ok) throw new Error('http '+res.status);
  const json = await res.json();
  const klines = json && json.data && json.data.klines;
  if(!klines || !klines.length) throw new Error('no data');
  const f = klines[klines.length-1].split(',');
  return {
    date:f[0], open:+f[1], close:+f[2], high:+f[3], low:+f[4],
    vol:+f[5], amount:+f[6], turnover:+f[10]
  };
}

async function pool(items, worker, concurrency){
  const results = new Array(items.length);
  let i = 0;
  async function run(){
    while(i < items.length){
      const idx = i++;
      try{ results[idx] = await worker(items[idx]); }
      catch(e){ results[idx] = { error: e.message }; }
      await new Promise(r=>setTimeout(r, DELAY_MS));
    }
  }
  await Promise.all(Array.from({length:concurrency}, run));
  return results;
}

async function main(){
  console.log('抓取股票列表...');
  let list = await fetchStockList();
  console.log(`全市场共 ${list.length} 只`);
  if(list.length === 0){
    console.error('股票列表是空的，多半是被目标接口拦截了，直接判定失败，不要静默"成功"');
    process.exit(1);
  }

  const testLimit = process.env.FETCH_LIMIT ? parseInt(process.env.FETCH_LIMIT, 10) : null;
  if(testLimit){
    list = list.slice(0, testLimit);
    console.log(`测试模式，只跑前 ${list.length} 只`);
  }

  fs.mkdirSync(DATA_DIR, { recursive:true });

  const results = await pool(list, async (item)=>{
    const secid = `${item.market}.${item.code}`;
    const bar = await fetchLatestBar(secid);
    return { code:item.code, bar };
  }, CONCURRENCY);

  let updated=0, failed=0;
  const dateCounts = {};
  results.forEach((r, i)=>{
    if(!r || r.error){ failed++; return; }
    const file = path.join(DATA_DIR, `${r.code}.json`);
    let arr = [];
    if(fs.existsSync(file)){
      try{ arr = JSON.parse(fs.readFileSync(file,'utf8')); }catch(e){ arr=[]; }
    }
    if(arr.length && arr[arr.length-1].date === r.bar.date){
      arr[arr.length-1] = r.bar;
    }else{
      arr.push(r.bar);
    }
    if(arr.length > MAX_DAYS_KEPT) arr = arr.slice(arr.length-MAX_DAYS_KEPT);
    fs.writeFileSync(file, JSON.stringify(arr));
    updated++;
    dateCounts[r.bar.date] = (dateCounts[r.bar.date]||0) + 1;
  });

  // 绝大多数股票这次抓到的应该是同一个交易日，取出现次数最多的那个日期作为"更新至"
  const lastUpdateDate = Object.entries(dateCounts).sort((a,b)=>b[1]-a[1])[0]?.[0] || null;
  fs.writeFileSync('data/meta.json', JSON.stringify({
    lastUpdateDate, updated, failed, total: list.length,
    ranAt: new Date().toISOString()
  }));

  console.log(`完成：更新 ${updated} 只，失败 ${failed} 只（失败率 ${(failed/list.length*100).toFixed(1)}%），数据日期 ${lastUpdateDate}`);
  if(failed/list.length > 0.3){
    console.error('失败率过高，可能是被限流了，建议检查');
    process.exit(1);
  }
}

main().catch(e=>{
  console.error('抓取失败：', e);
  process.exit(1);
});


