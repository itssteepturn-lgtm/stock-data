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
const DELAY_MS = 150;        // 抓个股数据时，每个请求之间留点间隔
const LIST_DELAY_MS = 300;  // 拉股票列表翻页间隔，从数据看放慢没什么用，缩短一点

// GitHub服务器发请求默认不带浏览器那种请求头，容易被当成明显的爬虫流量拦截
// （表现为安静地返回空数据，不是报错），所以显式伪装成浏览器
const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Referer': 'https://quote.eastmoney.com/',
  'Accept': 'application/json, text/plain, */*'
};

// 遇到临时性错误（502/503/超时之类）重试几次，不要一次抽风就整体失败
async function fetchWithRetry(url, retries=6){
  for(let attempt=1; attempt<=retries; attempt++){
    try{
      const res = await fetch(url, { headers: HEADERS });
      if(!res.ok){
        if(res.status>=500 && attempt<retries){
          await new Promise(r=>setTimeout(r, 1500*attempt));
          continue;
        }
        throw new Error('http '+res.status);
      }
      return await res.json();
    }catch(e){
      if(attempt>=retries) throw e;
      await new Promise(r=>setTimeout(r, 1500*attempt));
    }
  }
}

// 诊断用：只发一次超大页请求，把原始返回内容打印出来，直接看服务器到底怎么处理的，
// 不猜、不套重试逻辑
async function diagnoseSingleRequest(){
  const fs_filter = 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048';
  const url = `https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=6000&po=1&np=1`+
    `&fltt=2&invt=2&fid=f3&fs=${encodeURIComponent(fs_filter)}&fields=f12,f13,f14`;
  console.log('请求URL:', url);
  const res = await fetch(url, { headers: HEADERS });
  console.log('HTTP状态:', res.status, res.statusText);
  const text = await res.text();
  console.log('返回内容总长度:', text.length, '字符');
  console.log('前300字符:', text.slice(0, 300));
  try{
    const json = JSON.parse(text);
    console.log('data.total字段:', json && json.data && json.data.total);
    console.log('diff数组实际长度:', json && json.data && json.data.diff && json.data.diff.length);
  }catch(e){
    console.log('JSON解析失败:', e.message);
  }
}

async function fetchStockList(){
  const all = [];
  let pn = 1;
  const pz = 200; // 实测100肯定能用，试试200能不能减少一半的翻页次数
  // 沪深主板/中小板/创业板/科创板/北交所 股票（不含指数、不含B股/退市股）
  const fs_filter = 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048';
  const fields = 'f12,f13'; // 代码, 市场(0=深 1=沪)
  while(true){
    const url = `https://push2.eastmoney.com/api/qt/clist/get?pn=${pn}&pz=${pz}&po=1&np=1`+
      `&fltt=2&invt=2&fid=f3&fs=${encodeURIComponent(fs_filter)}&fields=${fields}`;
    const json = await fetchWithRetry(url);
    const list = (json && json.data && json.data.diff) || [];
    console.log(`  第${pn}页拿到 ${list.length} 条`);
    if(list.length === 0) break; // 真正翻到空页才算拿完
    all.push(...list);
    pn++;
    if(pn > 80) break; // 安全上限，80页*100条=8000，够覆盖全市场
    await new Promise(r=>setTimeout(r, LIST_DELAY_MS));
  }
  return all.map(q=>({ code:q.f12, market:q.f13 })).filter(x=>x.code && x.market!=null);
}

async function fetchLatestBar(secid){
  const url = `https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=${secid}`+
    `&fields1=f1,f2,f3,f4,f5&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61`+
    `&klt=101&fqt=1&end=20500101&lmt=2`;
  const json = await fetchWithRetry(url);
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

const LIST_CACHE_FILE = 'data/stock-list.json';

// 拉股票清单：整体失败就整体重试几次（不只是单页重试），每次之间留久一点冷却时间；
// 全都失败的话，退而求其次用仓库里存的"上次成功的清单"，这样单日的接口抽风
// 不会让整个采集彻底失败（缺点是当天新上市的股票会漏掉，但影响很小）
// 清单缓存超过这么多天没刷新，才会再去尝试重新拉一次最新的（跟上新股/退市），
// 平时都是直接用缓存，压根不去碰那个不稳定的接口
const LIST_REFRESH_DAYS = 14;

function loadCachedList(){
  if(!fs.existsSync(LIST_CACHE_FILE)) return null;
  try{
    const list = JSON.parse(fs.readFileSync(LIST_CACHE_FILE, 'utf8'));
    if(Array.isArray(list) && list.length >= 1000) return list;
  }catch(e){}
  return null;
}

// 清单缓存优先：只要有能用的缓存就直接用，不去反复碰那个不稳定的接口；
// 缓存太久没刷新，或者压根没有缓存，才会真的去现拉一次。
// 只试一次就够了——不行的话，反正每15分钟会有新一次运行自动重来，
// 没必要在一次运行里死等好几轮冷却，那样只是让这次运行变慢，成功率并不会更高
async function getStockList(){
  const cached = loadCachedList();
  const cacheAge = fs.existsSync(LIST_CACHE_FILE)
    ? (Date.now() - fs.statSync(LIST_CACHE_FILE).mtimeMs) / 86400000
    : Infinity;

  if(cached && cacheAge < LIST_REFRESH_DAYS){
    console.log(`用缓存的股票清单（${cacheAge.toFixed(1)}天前抓的，共${cached.length}只）`);
    return cached;
  }

  console.log(cached ? '缓存清单有点久了，尝试刷新一下...' : '还没有缓存清单，现拉一份...');
  try{
    const list = await fetchStockList();
    if(list.length < 1000) throw new Error('列表长度异常，只有'+list.length+'条');
    fs.mkdirSync(path.dirname(LIST_CACHE_FILE), { recursive:true });
    fs.writeFileSync(LIST_CACHE_FILE, JSON.stringify(list));
    return list;
  }catch(e){
    console.log(`这次拉取清单失败：${e.message}`);
  }
  if(cached){
    console.log('刷新失败，继续用手头的旧缓存清单顶着');
    return cached;
  }
  console.log('这次还没拿到清单，等下一次自动运行（每15分钟一次）再试');
  return null;
}

const BATCH_SIZE = 100;     // 每次跑只处理这么多只，抓完记进度，下次接着抓
const STATE_FILE = 'data/collect-state.json';

function loadState(){
  if(fs.existsSync(STATE_FILE)){
    try{ return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); }catch(e){}
  }
  return null;
}
function saveState(state){
  fs.mkdirSync(path.dirname(STATE_FILE), { recursive:true });
  fs.writeFileSync(STATE_FILE, JSON.stringify(state));
}

async function main(){
  if(process.env.DIAGNOSE){
    console.log('=== 诊断模式：只发一次请求，看原始返回 ===');
    await diagnoseSingleRequest();
    return;
  }

  const testLimit = process.env.FETCH_LIMIT ? parseInt(process.env.FETCH_LIMIT, 10) : null;

  // 测试模式：忽略断点续传状态，直接抓一小批看看通不通，不影响正式进度
  if(testLimit){
    console.log('抓取股票列表...');
    let list = await getStockList();
    if(!list){ console.log('这次没拿到清单，稍后再试'); return; }
    console.log(`全市场共 ${list.length} 只，测试模式只跑前 ${testLimit} 只`);
    list = list.slice(0, testLimit);
    fs.mkdirSync(DATA_DIR, { recursive:true });
    const results = await pool(list, async (item)=>{
      const bar = await fetchLatestBar(`${item.market}.${item.code}`);
      return { code:item.code, bar };
    }, CONCURRENCY);
    let updated=0, failed=0;
    results.forEach(r=>{
      if(!r||r.error){ failed++; return; }
      writeBarToFile(r.code, r.bar);
      updated++;
    });
    console.log(`测试完成：更新 ${updated} 只，失败 ${failed} 只`);
    return;
  }

  // 正式模式：断点续传，每次只处理一批，跑完一整轮之后要隔12小时以上才会开始新一轮
  // （交易日每天大概只会真正触发一次完整轮次：收盘后开始，慢慢抓到抓完为止）
  let state = loadState() || { inProgress:false, nextIndex:0, updatedTotal:0, failedTotal:0, dateCounts:{}, doneAt:null };

  if(!state.inProgress){
    if(state.doneAt){
      const hoursSince = (Date.now() - new Date(state.doneAt).getTime()) / 3600000;
      if(hoursSince < 12){
        console.log(`距离上一轮采集完成才过了 ${hoursSince.toFixed(1)} 小时，还不到12小时，本次跳过`);
        return;
      }
    }
    console.log('开始新一轮全市场采集');
    state = { inProgress:true, nextIndex:0, updatedTotal:0, failedTotal:0, dateCounts:{}, doneAt:null };
  }

  console.log('抓取股票列表...');
  const list = await getStockList();
  if(!list){
    console.log('这次没拿到清单（也没有缓存能顶），本次不推进进度，等下次自动运行再试');
    saveState(state); // 保留 inProgress:true，下次直接接着尝试，不会重新计时
    return;
  }
  console.log(`全市场共 ${list.length} 只，本轮进度 ${state.nextIndex}/${list.length}`);
  if(list.length === 0){
    console.error('股票列表是空的，本次先跳过，下次继续（不推进进度）');
    return;
  }

  const batch = list.slice(state.nextIndex, state.nextIndex + BATCH_SIZE);
  if(batch.length === 0){
    console.log('清单里已经没有更多要处理的了，直接标记本轮完成');
    finishRound(state, list.length);
    saveState(state);
    return;
  }

  console.log(`本次处理第 ${state.nextIndex+1} - ${state.nextIndex+batch.length} 只`);
  fs.mkdirSync(DATA_DIR, { recursive:true });

  const results = await pool(batch, async (item)=>{
    const bar = await fetchLatestBar(`${item.market}.${item.code}`);
    return { code:item.code, bar };
  }, CONCURRENCY);

  let batchUpdated=0, batchFailed=0;
  results.forEach(r=>{
    if(!r || r.error){ batchFailed++; return; }
    writeBarToFile(r.code, r.bar);
    batchUpdated++;
    state.dateCounts[r.bar.date] = (state.dateCounts[r.bar.date]||0) + 1;
  });

  state.nextIndex += batch.length;
  state.updatedTotal += batchUpdated;
  state.failedTotal += batchFailed;
  console.log(`本批完成：更新${batchUpdated}只，失败${batchFailed}只。累计进度 ${state.nextIndex}/${list.length}`);

  if(state.nextIndex >= list.length){
    finishRound(state, list.length);
  }
  saveState(state);
}

function writeBarToFile(code, bar){
  const file = path.join(DATA_DIR, `${code}.json`);
  let arr = [];
  if(fs.existsSync(file)){
    try{ arr = JSON.parse(fs.readFileSync(file,'utf8')); }catch(e){ arr=[]; }
  }
  if(arr.length && arr[arr.length-1].date === bar.date){
    arr[arr.length-1] = bar;
  }else{
    arr.push(bar);
  }
  if(arr.length > MAX_DAYS_KEPT) arr = arr.slice(arr.length-MAX_DAYS_KEPT);
  fs.writeFileSync(file, JSON.stringify(arr));
}

function finishRound(state, total){
  state.inProgress = false;
  state.doneAt = new Date().toISOString();
  const lastUpdateDate = Object.entries(state.dateCounts).sort((a,b)=>b[1]-a[1])[0]?.[0] || null;
  fs.writeFileSync('data/meta.json', JSON.stringify({
    lastUpdateDate, updated: state.updatedTotal, failed: state.failedTotal,
    total, ranAt: state.doneAt
  }));
  console.log(`本轮全市场采集彻底完成！更新${state.updatedTotal}只，失败${state.failedTotal}只，数据日期${lastUpdateDate}`);
}

main().catch(e=>{
  console.error('抓取失败：', e);
  process.exit(1);
});


