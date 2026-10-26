# Pipeline-for-Harlan

**消息列车。只搭载，不判断。**

本仓库是 Harlan 体系里的**搬运层**（服务名 `pipeline-gateway`，占用端口 18220）。它把各个生产者生成的注入内容，按字节贴进出站请求，然后转发给下游。整条链路上所有"该不该唤醒、该说什么、现在是什么状态"的判断，都不在这里。

单独成仓的原因：判断层（Cadence）和存储层（Lattice）可以各自升级、各自重启、各自回滚，搬运层不该跟着它们一起变版本。同时，搬运层未来要服务的不只一个生产者。

---

## 一、链路位置

```
         Operit / 客户端
                │  Bearer token
                ▼
    ┌───────────────────────┐
    │  pipeline-gateway     │  18220  0.0.0.0   ← 本仓库
    │  只搭载，不判断        │
    └───────┬───────┬───────┘
            │       │
   取块/快照│       │ 原样转发
            ▼       ▼
   ┌────────────┐  ┌──────────────────┐
   │  cadence   │  │  lattice gateway │ 18217 → 上游模型
   │  18221     │  └──────────────────┘
   │  127.0.0.1 │
   └────────────┘
```

- **本进程与 cadence 之间**：只在 loopback 上通话。`/pipeline/inject` 取块，`/pipeline/observe` 送对话快照，`/cadence/mcp` 透传 MCP。
- **本进程与 lattice 之间**：chat 请求改写后转发，其余路径原样透传。
- **对外只暴露 18220**。18217、18221 都不该直接暴露公网。

## 二、职责边界（改代码前先读这段）

允许做的事：

- 拉取注入内容，按顺序拼进请求体最后一条 user 消息
- 节流去重：签名没变就不重复贴
- 故障降级：生产者不可达时，**丢掉注入照常转发**，绝不阻断对话
- 路由：按路径前缀决定转发给谁

不允许做的事：

- 读情绪、算阈值、判断该不该唤醒
- 解析注入内容、改写措辞、截断补全
- 在生产者不可达时自己造一段内容顶上

任何"因为缺内容所以补一句"的冲动，都是越界。

## 三、路由表

| 路径 | 去向 | 方式 |
|---|---|---|
| `GET /pipeline/health` | 本地 | 免鉴权健康检查 |
| `/pipeline/*` | cadence | **仅 loopback**，外部 403 |
| `/cadence/mcp`、`/mcp` | cadence | 流式透传（SSE 长连接） |
| `POST /surf/finding` | cadence | 冲浪产物回投 |
| `POST /v1/chat/completions` | lattice | 收集 body → 贴块 → 转发 |
| 其余 | lattice | 原样流式转发 |

`/pipeline/*` 必须锁 loopback：外部若能调 `/pipeline/observe`，就能伪造对话污染状态。

## 四、多源规划

现在只有 cadence 一个生产者提供注入内容。后续要接入更多，原则如下：

1. **生产者只负责生成**，把内容放在自己的 HTTP 端点上，返回 `{ block, sig }`。
2. **搬运层按源顺序拼接**。每个源的内容自带自己的边界标记，搬运层不插入任何连接词、不做去重合并以外的处理。
3. **签名合并**：拼接后的 `sig` 是各源 `sig` 的连接串，任何一个源变了就重新贴。
4. **单源故障隔离**：某源超时只丢该源的内容，其余照常贴。

已规划接入的源：

| 源 | 状态 | 说明 |
|---|---|---|
| cadence | 已接入 | 五轴状态块、27 格语调 |
| surf | 待接入 | 冲浪产物。当前 surf 只做产物回投（`/surf/finding` → cadence），尚无主动注入端点；触发源设计确定后再定接口 |

接入新源需要改的只有三处：源配置、拉取函数、拼接函数。**不需要**在搬运层引入任何业务概念。

## 五、配置

复制 `.env.example` 为 `.env`（权限 600），关键项：

| 变量 | 默认 | 说明 |
|---|---|---|
| `PIPELINE_PORT` | 18232 | 灰度验证用 18232，稳定后改 18220 |
| `PIPELINE_TOKEN` | — | 与 cadence 同一枚，客户端不用改 |
| `UPSTREAM_BASE` | `http://127.0.0.1:18217` | lattice 网关 |
| `CADENCE_BASE` | `http://127.0.0.1:18221` | 注入源 |
| `INJECT_THROTTLE_SECONDS` | 1800 | 签名不变时的重复抑制窗口 |
| `BLOCK_CACHE_MS` | 30000 | 块缓存，避免每请求一次进程间调用 |
| `DEGRADE_COOLDOWN_MS` | 15000 | 源不可达后的冷却，避免每请求等超时 |
| `OBSERVE_ENABLED` | true | 是否把对话快照送给判定器 |

## 六、部署

```bash
# 1. 目录与配置
mkdir -p /root/pipeline-gateway/data
cp src/gateway.js /root/pipeline-gateway/gateway.js
cp .env.example /root/pipeline-gateway/.env && chmod 600 /root/pipeline-gateway/.env
vi /root/pipeline-gateway/.env        # 填三枚 token

# 2. 单元
cp deploy/pipeline-gateway.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now pipeline-gateway

# 3. 自检
node --check /root/pipeline-gateway/gateway.js
curl -s http://127.0.0.1:18220/pipeline/health
```

注意 Node 版本：本服务用 **`/usr/local/bin/node`（v22）**。系统默认的 `/usr/bin/node` 是 v18，是 cadence 的运行时，不要混用。

灰度切换的做法：先把 `PIPELINE_PORT` 设成 18232 起一份并行实例，客户端仍走旧入口；验证注入、MCP、SSE、降级都正常后，再改 18220 切流。

## 七、相关仓库

| 仓库 | 角色 | 端口 |
|---|---|---|
| `Pipeline-for-Harlan`（本仓） | 搬运 | 18220 |
| `Cadence-for-Harlan` | 判断 | 18221（loopback） |
| Lattice | 存储 / 过站 | 18217 |
