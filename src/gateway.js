#!/usr/bin/env node
/**
 * pipeline-gateway —— 注入消息搭载层
 *
 * 职责边界（硬约束，改代码前先读这段）：
 *   本进程**只做搬运**，不持有任何语义状态、不做任何判断。
 *   · 不读情绪、不算阈值、不判该不该唤醒
 *   · 不解析块的内容，只按字节拼进请求体
 *   · 唯一的"决策"是节流去重（同一状态签名在 N 秒内不重复注入）+ 故障降级
 *
 * 判断全在 cadence-bridge（引擎 / 判定器 / 块生成 / 主动唤醒）。
 * 本进程从 cadence 拉"当前块"，贴到出站请求上，再转发给 Lattice。
 *
 * 路由表：
 *   GET  /pipeline/health    本地健康（免鉴权）
 *   *    /cadence/mcp /mcp   转发 cadence（MCP，SSE 长连接透传）
 *   POST /surf/finding       转发 cadence（冲浪产物回投）
 *   *    /pipeline/*         转发 cadence（调试用）
 *   POST /v1/chat/completions 拉块 → 拼进最后一条 user → 转发 Lattice
 *   *    其余                 原样转发 Lattice
 */
'use strict';

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

// ── 极简 .env 读取（不引依赖）────────────────────────
function loadEnv(file) {
  try {
    const text = fs.readFileSync(file, 'utf8');
    for (const line of text.split('\n')) {
      const s = line.trim();
      if (!s || s.startsWith('#')) continue;
      const eq = s.indexOf('=');
      if (eq < 0) continue;
      const k = s.slice(0, eq).trim();
      let v = s.slice(eq + 1).trim();
      if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
        v = v.slice(1, -1);
      }
      if (process.env[k] === undefined) process.env[k] = v;
    }
  } catch { /* 无 .env 用环境变量 */ }
}
loadEnv(path.join(__dirname, '.env'));

const num = (k, d) => {
  const v = parseFloat(process.env[k]);
  return Number.isFinite(v) ? v : d;
};
const bool = (k, d) => {
  const v = process.env[k];
  if (v === undefined) return d;
  return v === 'true';
};

const CFG = {
  port: parseInt(process.env.PIPELINE_PORT || '18232', 10),
  host: process.env.PIPELINE_HOST || '0.0.0.0',
  token: process.env.PIPELINE_TOKEN || '',

  // 下游：Lattice 网关
  upstreamBase: process.env.UPSTREAM_BASE || 'http://127.0.0.1:18217',
  upstreamToken: process.env.UPSTREAM_TOKEN || '',

  // 上游：cadence 生产者
  cadenceBase: process.env.CADENCE_BASE || 'http://127.0.0.1:18221',
  cadenceToken: process.env.CADENCE_TOKEN || '',

  injectEnabled: bool('INJECT_ENABLED', true),
  // 块缓存：cadence 的块是实时生成的，缓存只为避免每请求一次进程间调用。
  // state 变化最快也在 tick（5min）级别，30s 足够且不会漏掉变化。
  blockCacheMs: num('BLOCK_CACHE_MS', 30000),
  blockTimeoutMs: num('BLOCK_TIMEOUT_MS', 3000),
  // 与 cadence 原来一致：状态签名变了才重贴，否则 N 秒内不重复
  injectThrottleSeconds: num('INJECT_THROTTLE_SECONDS', 1800),
  throttleEnabled: bool('INJECT_THROTTLE_ENABLED', true),
  // 故障冷却：cadence 不可达时，N 秒内不再重试，避免每请求都挂 3 秒
  degradeCooldownMs: num('DEGRADE_COOLDOWN_MS', 15000),
  observeEnabled: bool('OBSERVE_ENABLED', true),
  logFile: process.env.LOG_FILE || path.join(__dirname, 'data', 'pipeline.log'),
};

// ── 日志（与桥同格式：ISO-8601 UTC）──────────────────
function log(level, msg) {
  const line = `[${new Date().toISOString()}] [${level}] ${msg}`;
  console.log(line);
  try {
    fs.appendFileSync(CFG.logFile, line + '\n');
  } catch { /* 落盘失败不影响服务 */ }
}

// ── 注入节流（唯一有状态的逻辑，纯字符串比对）──────
let lastInjectSig = '';
let lastInjectAt = 0;
function shouldInject(sig) {
  if (!CFG.throttleEnabled) return true;
  const now = Date.now();
  const changed = sig !== lastInjectSig;
  const expired = (now - lastInjectAt) > CFG.injectThrottleSeconds * 1000;
  if (changed || expired) {
    lastInjectSig = sig;
    lastInjectAt = now;
    return true;
  }
  return false;
}

// ── 块拉取：缓存 + 并发合并 + 故障冷却 ──────────────
let cache = { payload: null, at: 0 };
let inflight = null;
let degradeUntil = 0;

function httpJson(base, pathname, method, token, bodyBuf, timeoutMs) {
  return new Promise((resolve, reject) => {
    const u = new URL(base + pathname);
    const lib = u.protocol === 'https:' ? https : http;
    const headers = { accept: 'application/json' };
    if (token) headers.authorization = 'Bearer ' + token;
    if (bodyBuf) {
      headers['content-type'] = 'application/json';
      headers['content-length'] = Buffer.byteLength(bodyBuf);
    }
    const req = lib.request({
      hostname: u.hostname, port: u.port || 80, path: u.pathname + u.search,
      method, headers, timeout: timeoutMs,
    }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        try { resolve({ status: res.statusCode, json: JSON.parse(Buffer.concat(chunks).toString('utf8')) }); }
        catch (e) { reject(new Error('bad json from ' + pathname)); }
      });
    });
    req.on('timeout', () => { req.destroy(new Error('timeout after ' + timeoutMs + 'ms')); });
    req.on('error', reject);
    if (bodyBuf) req.write(bodyBuf);
    req.end();
  });
}

async function fetchBlock() {
  const now = Date.now();
  if (cache.payload && (now - cache.at) < CFG.blockCacheMs) return cache.payload;
  if (now < degradeUntil) return { block: '', sig: '', degraded: true };
  if (inflight) return inflight;

  inflight = (async () => {
    try {
      const r = await httpJson(CFG.cadenceBase, '/pipeline/inject', 'GET', CFG.cadenceToken, null, CFG.blockTimeoutMs);
      if (r.status !== 200 || !r.json) throw new Error('cadence returned ' + r.status);
      const payload = {
        block: typeof r.json.block === 'string' ? r.json.block : '',
        sig: typeof r.json.sig === 'string' ? r.json.sig : '',
      };
      cache = { payload, at: Date.now() };
      return payload;
    } catch (e) {
      degradeUntil = Date.now() + CFG.degradeCooldownMs;
      log('WARN', `inject source unavailable, forwarding without block (${e.message})`);
      return { block: '', sig: '', degraded: true };
    } finally {
      inflight = null;
    }
  })();
  return inflight;
}

// ── 对话快照推送（异步、不阻塞、失败不重试）────────
function observeAsync(messages) {
  if (!CFG.observeEnabled || !Array.isArray(messages) || messages.length === 0) return;
  const bodyBuf = Buffer.from(JSON.stringify({ messages }), 'utf8');
  httpJson(CFG.cadenceBase, '/pipeline/observe', 'POST', CFG.cadenceToken, bodyBuf, CFG.blockTimeoutMs)
    .then((r) => {
      if (r.status !== 200) log('WARN', 'observe rejected: ' + r.status);
    })
    .catch((e) => log('WARN', 'observe failed: ' + e.message));
}

// ── 纯转发（流式透传，不缓冲）──────────────────────
// bodyBuf 为 null 时走流式管道（MCP / SSE / 大 body 都靠这条），
// 不能在非 chat 分支提前收集 body，否则 MCP 的 JSON-RPC 报文会丢。
function proxyTo(base, pathname, search, method, headers, bodyBuf, res, token, clientReq) {
  const u = new URL(base + pathname + (search || ''));
  const lib = u.protocol === 'https:' ? https : http;

  const outHeaders = { ...headers };
  delete outHeaders['host'];
  delete outHeaders['content-length'];
  delete outHeaders['authorization'];
  if (token) outHeaders['authorization'] = 'Bearer ' + token;
  if (bodyBuf && bodyBuf.length) outHeaders['content-length'] = Buffer.byteLength(bodyBuf);

  const req = lib.request({
    hostname: u.hostname,
    port: u.port || (u.protocol === 'https:' ? 443 : 80),
    path: u.pathname + u.search,
    method,
    headers: outHeaders,
  }, (upRes) => {
    res.writeHead(upRes.statusCode, upRes.headers);
    upRes.pipe(res);
  });

  req.on('error', (e) => {
    log('ERROR', `upstream error ${base}${pathname}: ${e.message}`);
    if (!res.headersSent) {
      res.writeHead(502, { 'content-type': 'application/json' });
    }
    res.end(JSON.stringify({ error: { message: 'pipeline upstream unreachable: ' + e.message } }));
  });

  if (bodyBuf && bodyBuf.length) {
    req.write(bodyBuf);
    req.end();
  } else if (clientReq && method !== 'GET' && method !== 'HEAD') {
    clientReq.pipe(req);          // 流式透传请求体（MCP JSON-RPC / 大 body）
  } else {
    req.end();
  }
}

// ── 拼装（唯一改写请求体的地方）────────────────────
function injectIntoBody(body, block) {
  if (!body || !Array.isArray(body.messages) || body.messages.length === 0) {
    return { body, injected: false };
  }
  let idx = -1;
  for (let i = body.messages.length - 1; i >= 0; i--) {
    if (body.messages[i] && body.messages[i].role === 'user') { idx = i; break; }
  }
  if (idx === -1) return { body, injected: false };

  const msg = body.messages[idx];
  if (typeof msg.content === 'string') {
    body.messages[idx] = { ...msg, content: block + '\n\n' + msg.content };
  } else if (Array.isArray(msg.content)) {
    body.messages[idx] = { ...msg, content: [{ type: 'text', text: block + '\n' }, ...msg.content] };
  } else {
    return { body, injected: false };
  }
  return { body, injected: true };
}

// ── 路由判定（纯字符串匹配，无业务含义）────────────
const MCP_PATHS = new Set(
  (process.env.MCP_PATHS || '/cadence/mcp,/cadence/mcp/,/mcp,/mcp/')
    .split(',').map((s) => s.trim()).filter(Boolean)
);
const SURF_PATH = process.env.SURF_FINDING_PATH || '/surf/finding';

function isChatPath(p) {
  return /\/v1\/chat\/completions$/.test(p) || /\/chat\/completions$/.test(p);
}
function goesToCadence(p) {
  return MCP_PATHS.has(p) || p === SURF_PATH;
}

// /pipeline/* 是内部接口（拉块 / 喂判定器），不对外暴露。
// 只允许本机访问：外部若能调 /pipeline/observe，就能伪造对话污染状态。
function isLoopbackAddr(addr) {
  return addr === '127.0.0.1' || addr === '::1' || addr === '::ffff:127.0.0.1';
}

// ── 主服务 ────────────────────────────────────────
const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://localhost');

  // SSE 长连接必需：Node 默认 300s 会掐断 MCP 流
  req.setTimeout(0);
  if (res.setTimeout) res.setTimeout(0);

  if (u.pathname === '/pipeline/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok', service: 'pipeline-gateway', version: '1.0.0',
      role: 'transport-only',
      cadence: CFG.cadenceBase, upstream: CFG.upstreamBase,
      block_cached: !!cache.payload, degraded: Date.now() < degradeUntil,
    }));
    return;
  }

  // 鉴权：与桥同一枚 token，客户端不用改
  const given = (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '');
  if (CFG.token && given !== CFG.token) {
    res.writeHead(401, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'unauthorized' } }));
    return;
  }

  // /pipeline/* 内部接口：仅本机可达
  if (u.pathname.startsWith('/pipeline/')) {
    if (!isLoopbackAddr(req.socket.remoteAddress)) {
      res.writeHead(403, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { message: 'internal endpoint, loopback only' } }));
      return;
    }
    proxyTo(CFG.cadenceBase, u.pathname, u.search, req.method, req.headers, null, res, CFG.cadenceToken, req);
    return;
  }

  // 非 chat 且非本地路径：直接流式转发，不收集 body（MCP 需要）
  if (!isChatPath(u.pathname)) {
    const toCad = goesToCadence(u.pathname);
    proxyTo(toCad ? CFG.cadenceBase : CFG.upstreamBase, u.pathname, u.search,
      req.method, req.headers, null, res,
      toCad ? CFG.cadenceToken : CFG.upstreamToken, req);
    return;
  }

  // chat completions：收集 body → 拼块 → 转发
  const chunks = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', async () => {
    const bodyBuf = Buffer.concat(chunks);

    if (!CFG.injectEnabled || bodyBuf.length === 0) {
      proxyTo(CFG.upstreamBase, u.pathname, u.search, req.method, req.headers, bodyBuf, res, CFG.upstreamToken, req);
      return;
    }

    let body;
    try {
      body = JSON.parse(bodyBuf.toString('utf8'));
    } catch {
      proxyTo(CFG.upstreamBase, u.pathname, u.search, req.method, req.headers, bodyBuf, res, CFG.upstreamToken, req);
      return;
    }

    // 先取快照再注入：顺序不能反，否则判定器读到的是带块文本
    observeAsync(body.messages);

    let injected = false;
    try {
      const { block, sig } = await fetchBlock();
      if (block && shouldInject(sig)) {
        const r = injectIntoBody(body, block);
        body = r.body;
        injected = r.injected;
      }
    } catch (e) {
      log('WARN', 'inject skipped: ' + e.message);
    }

    const outBuf = Buffer.from(JSON.stringify(body), 'utf8');
    log('INFO', `inject=${injected} win=${req.headers['x-lattice-window-id'] || 'main'}`);
    proxyTo(CFG.upstreamBase, u.pathname, u.search, req.method, req.headers, outBuf, res, CFG.upstreamToken, req);
  });
});

try { fs.mkdirSync(path.dirname(CFG.logFile), { recursive: true }); } catch { /* ignore */ }

server.listen(CFG.port, CFG.host, () => {
  log('INFO', `pipeline-gateway listening on ${CFG.host}:${CFG.port}`);
  log('INFO', `  cadence  ← ${CFG.cadenceBase}  (inject source + MCP + surf)`);
  log('INFO', `  upstream → ${CFG.upstreamBase}  (lattice gateway)`);
  log('INFO', `  inject=${CFG.injectEnabled} throttle=${CFG.throttleEnabled ? CFG.injectThrottleSeconds + 's' : 'off'} cache=${CFG.blockCacheMs}ms`);
});
