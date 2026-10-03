import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine } from '../src/engine.js';
import { SqliteLedger } from '../src/sqlite-ledger.js';
import { MiniMatcher, type MiniOutcome } from './mini-matcher.js';
import type { DomainEvent, OrderCommand, Receipt } from '../src/types.js';

function newEngine(dbDir: string, snapshotInterval = 0) {
  const ledger = new SqliteLedger(join(dbDir, 'm.db'));
  const engine = new Engine({ ledger, snapshotInterval });
  return { engine, ledger };
}

function tempDir(): string {
  return mkdtempSync(join(tmpdir(), 'sim-market-'));
}

type AnyCmd = OrderCommand;

/** 把引擎回执翻译成与 mini outcome 同构的结构进行比对 */
function receiptToOutcome(rc: Receipt): MiniOutcome {
  if (rc.ok) {
    const trades = rc.events.filter(
      (e): e is Extract<DomainEvent, { type: 'Trade' }> => e.type === 'Trade',
    );
    return {
      accepted: true,
      consumeReceive: false, // 由调用方按水位另算
      trades: trades.map((t) => ({
        tradeId: t.tradeId,
        price: t.price,
        qty: t.qty,
        makerId: t.makerOrderId,
        takerId: t.takerOrderId,
      })),
      orderId: rc.orderId,
      status: rc.status === 'CANCELLED' ? 'CANCELLED' : rc.status,
      tradedQty: rc.tradedQty,
      restingQty: rc.restingQty,
    };
  }
  return {
    accepted: false,
    consumeReceive: false,
    trades: [],
    orderId: 0,
    status: 'REJECTED',
    tradedQty: 0,
    restingQty: 0,
    errorCode: rc.error.code,
  };
}

function assertSameOutcome(a: MiniOutcome, b: MiniOutcome, label: string) {
  assert.equal(a.accepted, b.accepted, `${label}: accepted`);
  if (!a.accepted) {
    assert.equal(a.errorCode, b.errorCode, `${label}: errorCode`);
    return;
  }
  assert.equal(a.orderId, b.orderId, `${label}: orderId`);
  assert.equal(a.status, b.status, `${label}: status`);
  assert.equal(a.tradedQty, b.tradedQty, `${label}: tradedQty`);
  assert.equal(a.restingQty, b.restingQty, `${label}: restingQty`);
  assert.equal(a.trades.length, b.trades.length, `${label}: trade count`);
  a.trades.forEach((t, i) => {
    const u = b.trades[i]!;
    assert.equal(t.tradeId, u.tradeId, `${label}: tradeId[${i}]`);
    assert.equal(t.price, u.price, `${label}: price[${i}]`);
    assert.equal(t.qty, u.qty, `${label}: qty[${i}]`);
    assert.equal(t.makerId, u.makerId, `${label}: maker[${i}]`);
    assert.equal(t.takerId, u.takerId, `${label}: taker[${i}]`);
  });
}

function assertSameBook(
  engine: Engine,
  mini: MiniMatcher,
  label: string,
): void {
  const snap = engine.buildSnapshot();
  const miniLevels = mini.levels();
  assert.deepEqual(snap.bids, miniLevels.bids, `${label}: bids`);
  assert.deepEqual(snap.asks, miniLevels.asks, `${label}: asks`);
}

/** 把一条命令同时喂给两边，自己负责推进两侧的接收序号水位 */
function runBoth(
  engine: Engine,
  mini: MiniMatcher,
  miniReceive: { v: number },
  engineReceive: { v: number },
  key: string,
  cmd: AnyCmd,
): { engine: Receipt; mini: MiniOutcome } {
  const beforeMini = miniReceive.v;
  const rc = engine.submit(key, cmd);
  if (rc.ok) engineReceive.v = rc.receiveSeq;

  const candidate = beforeMini + 1;
  let out: MiniOutcome;
  if (cmd.kind === 'place') {
    out = mini.place(
      candidate,
      cmd.side,
      cmd.price,
      cmd.qty,
      cmd.tif,
    );
    if (out.accepted) miniReceive.v = candidate;
  } else if (cmd.kind === 'cancel') {
    out = mini.cancel(cmd.orderId);
  } else {
    out = mini.amend(cmd.orderId, candidate, {
      price: cmd.price,
      qty: cmd.qty,
    });
    if (out.accepted && out.consumeReceive) miniReceive.v = candidate;
  }
  return { engine: rc, mini: out };
}

test('部分成交：IOC 吃多个价位，剩余撤销；小模型逐笔一致', () => {
  const dir = tempDir();
  try {
    const { engine, ledger } = newEngine(dir);
    const mini = new MiniMatcher();
    const miniR = { v: 0 };
    const engR = { v: 0 };

    const cmds: Array<[string, AnyCmd]> = [
      ['k1', { kind: 'place', side: 'SELL', price: 101, qty: 3, tif: 'GTC' }],
      ['k2', { kind: 'place', side: 'SELL', price: 100, qty: 2, tif: 'GTC' }],
      ['k3', { kind: 'place', side: 'SELL', price: 100, qty: 4, tif: 'GTC' }],
      // 买 8@100 IOC：先吃价位 100 上的 2+4=6（按序号），再剩 2 无价位可吃 -> 撤销
      ['k4', { kind: 'place', side: 'BUY', price: 100, qty: 8, tif: 'IOC' }],
    ];
    cmds.forEach(([k, c], i) => {
      const { engine: rc, mini: out } = runBoth(
        engine,
        mini,
        miniR,
        engR,
        k,
        c,
      );
      assertSameOutcome(receiptToOutcome(rc), out, `cmd#${i}`);
      assertSameBook(engine, mini, `cmd#${i}`);
    });

    // 显式核对 k4 的部分成交：2 笔成交共 6，剩余 2 撤销
    const k4 = engine.submit('k4', {
      kind: 'place',
      side: 'BUY',
      price: 100,
      qty: 8,
      tif: 'IOC',
    });
    assert.equal(k4.ok, true);
    if (k4.ok) {
      assert.equal(k4.tradedQty, 6);
      assert.equal(k4.restingQty, 0);
      assert.equal(k4.status, 'EXPIRED');
      const trades = k4.events.filter((e) => e.type === 'Trade');
      assert.equal(trades.length, 2);
      // 同价位按接收序号：订单 2（seq2）先于订单 3（seq3）
      assert.deepEqual(
        trades.map((t) => (t.type === 'Trade' ? t.makerOrderId : -1)),
        [2, 3],
      );
    }
    ledger.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('改单：改价/加量失去排队位置，仅减量保持原位', () => {
  const dir = tempDir();
  try {
    const { engine, ledger } = newEngine(dir);
    const mini = new MiniMatcher();
    const miniR = { v: 0 };
    const engR = { v: 0 };

    const cmds: Array<[string, AnyCmd]> = [
      ['a', { kind: 'place', side: 'SELL', price: 100, qty: 5, tif: 'GTC' }], // id1
      ['b', { kind: 'place', side: 'SELL', price: 100, qty: 5, tif: 'GTC' }], // id2
      // id1 仅减量：仍排 id2 前
      ['c', { kind: 'amend', orderId: 1, qty: 4 }],
      // 买 3@100：应与 id1 成交（减量后仍队首）
      ['d', { kind: 'place', side: 'BUY', price: 100, qty: 3, tif: 'GTC' }],
      // id2 加量：排到同价位队尾（目前只有它，效果不变但消耗新序号）
      ['e', { kind: 'amend', orderId: 2, qty: 9 }],
      // 新卖单 id6@100 与 id2(seq5) 同价位；id2 加量后 seq=5 仍早于 id6
      ['f', { kind: 'place', side: 'SELL', price: 100, qty: 2, tif: 'GTC' }],
      // 买 9@100：剩 id2=9（原5，加量到9，未成交）先吃完 9，id6 不动
      ['g', { kind: 'place', side: 'BUY', price: 100, qty: 9, tif: 'IOC' }],
      // id1 改价到 99 立即跨越卖？id1 是卖方，改低价会与买盘成交
      // 此时买盘为空（d 的买单成交后成为 FULL FILLED maker? d 是 GTC 买3，成交3 全部）
      ['h', { kind: 'amend', orderId: 6, price: 99, qty: 2 }],
    ];
    cmds.forEach(([k, c], i) => {
      const { engine: rc, mini: out } = runBoth(
        engine,
        mini,
        miniR,
        engR,
        k,
        c,
      );
      assertSameOutcome(receiptToOutcome(rc), out, `cmd#${i}`);
      assertSameBook(engine, mini, `cmd#${i}`);
    });
    ledger.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('差分随机场景：大量交错挂单/成交/取消/改单，两侧订单簿始终一致', () => {
  const dir = tempDir();
  try {
    const { engine, ledger } = newEngine(dir);
    const mini = new MiniMatcher();
    const miniR = { v: 0 };
    const engR = { v: 0 };

    let seed = 0xc0ffee;
    const rand = () => {
      // 确定性 LCG
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return seed / 0x7fffffff;
    };

    let keyCounter = 0;
    // 先收集两侧“可能存活”的订单号（新挂单号在两侧同为当前接收序号+1）
    for (let i = 0; i < 300; i++) {
      const roll = rand();
      let cmd: AnyCmd;
      if (roll < 0.55) {
        const side = rand() < 0.5 ? 'BUY' : 'SELL';
        const price = 90 + Math.floor(rand() * 21); // 90..110
        const qty = 1 + Math.floor(rand() * 8);
        const tif = rand() < 0.75 ? 'GTC' : 'IOC';
        cmd = { kind: 'place', side, price, qty, tif };
      } else if (roll < 0.8) {
        // 取消一个历史订单号
        const id = 1 + Math.floor(rand() * (miniR.v || 1));
        cmd = { kind: 'cancel', orderId: id };
      } else {
        const id = 1 + Math.floor(rand() * (miniR.v || 1));
        const patch: { price?: number; qty?: number } = {};
        if (rand() < 0.5) patch.price = 90 + Math.floor(rand() * 21);
        if (rand() < 0.5) patch.qty = 1 + Math.floor(rand() * 10);
        if (patch.price === undefined && patch.qty === undefined) {
          patch.qty = 1 + Math.floor(rand() * 10);
        }
        cmd = { kind: 'amend', orderId: id, ...patch };
      }

      const key = `r${keyCounter++}`;
      const { engine: rc, mini: out } = runBoth(
        engine,
        mini,
        miniR,
        engR,
        key,
        cmd,
      );
      assertSameOutcome(receiptToOutcome(rc), out, `iter#${i}`);
      if (i % 17 === 0) assertSameBook(engine, mini, `iter#${i}`);
    }
    assertSameBook(engine, mini, 'final');
    ledger.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('取消未知/已终结订单与非法参数两侧一致', () => {
  const dir = tempDir();
  try {
    const { engine, ledger } = newEngine(dir);
    const mini = new MiniMatcher();
    const miniR = { v: 0 };
    const engR = { v: 0 };

    const cmds: Array<[string, AnyCmd]> = [
      ['x1', { kind: 'cancel', orderId: 1 }],
      ['x2', { kind: 'amend', orderId: 1, qty: 5 }],
      ['x3', { kind: 'place', side: 'BUY', price: 0, qty: 1, tif: 'GTC' }],
      ['x4', { kind: 'place', side: 'BUY', price: 10, qty: -2, tif: 'IOC' }],
      [
        'x5',
        { kind: 'place', side: 'BUY', price: 10, qty: 2, tif: 'GTC' },
      ], // id1
      ['x6', { kind: 'cancel', orderId: 1 }],
      ['x7', { kind: 'cancel', orderId: 1 }], // 已终结
      ['x8', { kind: 'amend', orderId: 1, qty: 9 }],
    ];
    cmds.forEach(([k, c], i) => {
      const { engine: rc, mini: out } = runBoth(
        engine,
        mini,
        miniR,
        engR,
        k,
        c,
      );
      assertSameOutcome(receiptToOutcome(rc), out, `cmd#${i}`);
    });
    ledger.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
