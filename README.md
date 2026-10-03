# sim-market —— 单一模拟市场限价订单簿服务

TypeScript 实现的单市场模拟交易所：限价挂单（GTC）、立即成交剩余即撤销（IOC）、
撤单与改单；SQLite 事件账本 + WebSocket 实时发布。撮合器、账本、发布器围绕
**同一提交序号（commitSeq）**推进。

## 规则

### 撮合优先级
- **价格优先**：买单取最高买价、卖单取最低卖价；
- **接收序号优先（FIFO）**：同价位按服务端接收序号 `prioritySeq` 小者先成交；
- 改单（amend）只要**改价或增加数量**，即失去原排队位置（分配新接收序号，
  排到同价位队尾）；仅减量（或价格/数量都不变）保持原位置。

### 两种订单
- `GTC`：未成交部分挂入订单簿；
- `IOC`：立即撮合，未成交部分撤销（`EXPIRED`）。

### 序号与事务不变量
1. `receiveSeq`：服务端接收序号，仅在“新挂单”与“改价/加量导致重排队”时递增，
   同时充当订单号与排队优先级；
2. `commitSeq`：提交序号，只有**被接受、真正推进账本**的命令才占号；
   业务拒绝（未知单/非法参数）不占号、不发布，但其拒绝回执写入幂等表；
3. 撮合在内存克隆态上纯计算 → SQLite 单事务落盘 → **落盘成功后**才推进内存状态、
   接收序号水位并广播；落盘失败三者都不动（返回 `LEDGER_UNAVAILABLE`，可用原键重试）；
4. 重启只按事件流重放（Trade 事件带账本里的 `tradeId`），**不重新撮合**，
   因此不会再次生成已存在的成交；
5. 幂等键：相同键 + 相同载荷返回历史回执（不占任何序号）；
   相同键 + 不同载荷返回 `IDEMPOTENCY_KEY_CONFLICT`。

### 订阅续读
- 订阅消息：`{"type":"subscribe","lastSeq":N}`；
- 缺口仍在账本保留范围内：直接按序号补发 `commits`；
- 缺口超出保留范围（已被快照裁剪）：先发送持久化保留的 `snapshot`，
  再补发快照序号之后的 `commits`；
- `lastSeq` 超过当前头：返回 `replay_error/AHEAD_OF_HEAD`。

## 运行

```bash
npm install          # better-sqlite3 为原生模块，需要可用的 C/C++ 工具链
npm test             # tsc 编译 + node --test（11 个测试）
PORT=8080 DB_PATH=./market.db SNAPSHOT_INTERVAL=50 npm start
```

## HTTP 接口

所有写命令必须带 `Idempotency-Key` 头。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/orders` | 挂单 `{side:"BUY"\|"SELL", price:int, qty:int, tif:"GTC"\|"IOC"}` |
| POST | `/orders/:id/cancel` | 撤单 |
| POST | `/orders/:id/amend` | 改单 `{price?:int, qty?:int}`（至少一个） |
| GET | `/snapshot` | 当前订单簿快照（含 bids/asks 聚合档位） |
| GET | `/health` | 序号水位 |
| WS | `/ws` | 文本帧订阅，首条消息为 subscribe |

回执示例（成功）：

```json
{
  "ok": true,
  "requestKey": "k-123",
  "receiveSeq": 7,
  "events": [{ "type": "Trade", "tradeId": "T7-1", "qty": 2 }],
  "orderId": 7,
  "status": "FILLED",
  "tradedQty": 5,
  "restingQty": 0
}
```

错误状态码：`400 INVALID_ARGUMENT`、`404 UNKNOWN_ORDER`、
`409 IDEMPOTENCY_KEY_CONFLICT`、`503 LEDGER_UNAVAILABLE`。

## 代码结构

```
src/
  types.ts         命令/事件/回执/快照/Ledger 接口
  matcher.ts       纯内存撮合状态机（唯一推进方式 = applyEvent）
  engine.ts        撮合器 × SQLite 账本 × 发布器的提交序号编排
  sqlite-ledger.ts SQLite 事件账本（同步事务、快照裁剪、故障注入）
  publisher.ts     按序号续读/快照恢复的订阅发布器
  protocol.ts      HTTP 请求体校验
  websocket.ts     最小 RFC6455 文本帧实现（含 upgrade head 处理）
  server.ts        HTTP + WS 启动入口
test/
  mini-matcher.ts  独立小型撮合模型（与生产代码不共享实现，作为对照 oracle）
  matching.test.ts 部分成交/改单位置 + 差分随机测试（300 条交错命令逐笔比对）
  durability.test.ts 幂等、落盘失败联锁、重启重放不重复成交、快照缺口恢复
  e2e.test.ts      真实 HTTP + 真实 WebSocket（原始 socket 握手/帧）端到端
```

## 关键测试

- **独立小模型核对部分成交**：`MiniMatcher` 用朴素数组重写整套规则，
  差分测试把同一随机命令流分别喂给真引擎（SQLite + 事件重放）与小模型，
  逐笔比对 tradeId、价格、数量、对手方与聚合档位；
- **交错场景**：成交、取消、改单与注入的落盘失败混合执行后重启，
  断言成交事件数量与 `tradeId` 集合与重启前完全一致；
- **故障联锁**：注入磁盘故障期间撮合状态/序号/发布均不推进，原键重试成功。
