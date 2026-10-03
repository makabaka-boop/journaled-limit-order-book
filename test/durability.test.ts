import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine } from '../src/engine.js';
import { SqliteLedger } from '../src/sqlite-ledger.js';
import { Publisher } from '../src/publisher.js';
import type { OrderCommand } from '../src/types.js';

function tempDir(): string {
  return mkdtempSync(join(tmpdir(), 'sim-market-'));
}

const buy = (
  price: number,
  qty: number,
  tif: 'GTC' | 'IOC' = 'GTC',
): OrderCommand => ({ kind: 'place', side: 'BUY', price, qty, tif });
const sell = (
  price: number,
  qty: number,
  tif: 'GTC' | 'IOC' = 'GTC',
): OrderCommand => ({ kind: 'place', side: 'SELL', price, qty, tif });

test('幂等键：重复键返回原回执（含原成交 tradeId），不占新序号', () => {
  const dir = tempDir();
  try {
    const ledger = new SqliteLedger(join(dir, 'm.db'));
    const engine = new Engine({ ledger });

    engine.submit('sell-1', sell(100, 5));
    const first = engine.submit('buy-1', buy(100, 3, 'IOC'));
    assert.equal(first.ok, true);
    if (first.ok) {
      assert.equal(first.tradedQty, 3);
      assert.equal(first.receiveSeq, 2);
    }

    const headAfterFirst = engine.currentCommitSeq;
    const receiveAfterFirst = engine.currentReceiveSeq;

    // 相同键 + 相同载荷：返回完全相同的回执
    const again = engine.submit('buy-1', buy(100, 3, 'IOC'));
    assert.deepEqual(again, first);
    assert.equal(engine.currentCommitSeq, headAfterFirst); // 无新提交
    assert.equal(engine.currentReceiveSeq, receiveAfterFirst);

    // 相同键 + 改变载荷：拒绝（不是按新命令成交）
    const conflict = engine.submit('buy-1', buy(101, 3, 'IOC'));
    assert.equal(conflict.ok, false);
    if (!conflict.ok) {
      assert.equal(conflict.error.code, 'IDEMPOTENCY_KEY_CONFLICT');
    }
    assert.equal(engine.currentCommitSeq, headAfterFirst);

    // 账本里该键仍然只有第一次的成交，没有因为重试多撮合一次
    const { commits } = ledger.replay();
    const buyCommits = commits.filter((c) => c.requestKey === 'buy-1');
    assert.equal(buyCommits.length, 1);
    const trades = buyCommits[0]!.events.filter((e) => e.type === 'Trade');
    assert.equal(trades.length, 1);
    ledger.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('落盘失败：不推进撮合状态/序号，不发布成交；原键重试后成功', () => {
  const dir = tempDir();
  try {
    const ledger = new SqliteLedger(join(dir, 'm.db'));
    const published: number[] = [];
    const engine = new Engine({
      ledger,
      publisher: {
        publishCommitted: (seq: number) => {
          published.push(seq);
        },
      } as unknown as Publisher,
    });

    engine.submit('sell-1', sell(100, 5));
    const head = engine.currentCommitSeq;

    ledger.injectFault(new Error('disk full'));
    const failed = engine.submit('buy-1', buy(100, 3, 'IOC'));
    assert.equal(failed.ok, false);
    if (!failed.ok) assert.equal(failed.error.code, 'LEDGER_UNAVAILABLE');

    // 序号水位不动
    assert.equal(engine.currentCommitSeq, head);
    // 卖单剩余仍是 5（内存里没有发生成交）
    assert.deepEqual(engine.buildSnapshot().asks, [{ price: 100, qty: 5 }]);
    // 发布器没有收到任何成交
    assert.deepEqual(published, [1]);

    ledger.clearFault();
    // 原键重试：这次成交成功（不是重复键冲突，因为失败时没写请求表）
    const recovered = engine.submit('buy-1', buy(100, 3, 'IOC'));
    assert.equal(recovered.ok, true);
    if (recovered.ok) {
      assert.equal(recovered.tradedQty, 3);
      assert.equal(recovered.receiveSeq, 2);
    }
    assert.deepEqual(engine.buildSnapshot().asks, [{ price: 100, qty: 2 }]);
    assert.deepEqual(published, [1, 2]);

    // 再次原键重试：拿到同一回执，不会再成交
    const duplicate = engine.submit('buy-1', buy(100, 3, 'IOC'));
    assert.deepEqual(duplicate, recovered);
    assert.deepEqual(engine.buildSnapshot().asks, [{ price: 100, qty: 2 }]);
    ledger.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('交错：成交/取消/落盘失败混合后重启重放，成交不会再次生成', () => {
  const dir = tempDir();
  const dbFile = join(dir, 'm.db');
  try {
    const ledger = new SqliteLedger(dbFile);
    const engine = new Engine({ ledger });

    engine.submit('s1', sell(100, 5)); // seq1, order1
    engine.submit('s2', sell(99, 2)); // seq2, order2（最优卖）
    engine.submit('b1', buy(100, 4, 'IOC')); // seq3: 吃 order2 全部2 + order1 部分2
    engine.submit('c1', { kind: 'cancel', orderId: 1 }); // seq4: 撤 order1 剩余1

    // 记录此刻预期
    const expectedTrades = [
      { price: 99, qty: 2, maker: 2, taker: 3 },
      { price: 100, qty: 2, maker: 1, taker: 3 },
    ];
    const { commits: beforeRestart } = ledger.replay();
    const persistedTrades = beforeRestart.flatMap((c) =>
      c.events.filter((e) => e.type === 'Trade'),
    );
    assert.deepEqual(
      persistedTrades.map((t) =>
        t.type === 'Trade'
          ? { price: t.price, qty: t.qty, maker: t.makerOrderId, taker: t.takerOrderId }
          : null,
      ),
      expectedTrades,
    );

    // 落盘失败的命令不应出现在重放里
    ledger.injectFault();
    const failedCancel = engine.submit('c2', { kind: 'cancel', orderId: 2 });
    assert.equal(failedCancel.ok, false);
    ledger.clearFault();

    ledger.close();

    // 重启：新引擎重放同一账本
    const ledger2 = new SqliteLedger(dbFile);
    const engine2 = new Engine({ ledger: ledger2 });
    const { commits: afterRestart, snapshot } = ledger2.replay();
    assert.equal(snapshot, null);
    assert.equal(afterRestart.length, beforeRestart.length);

    // 重放得到的成交事件数量/内容与之前完全相同（不重新撮合）
    const replayTrades = afterRestart.flatMap((c) =>
      c.events.filter((e) => e.type === 'Trade'),
    );
    assert.equal(replayTrades.length, persistedTrades.length);
    assert.deepEqual(
      replayTrades.map((t) => t.type === 'Trade' && t.tradeId),
      persistedTrades.map((t) => t.type === 'Trade' && t.tradeId),
    );

    // 订单簿状态与重启前一致：order1 已取消（剩1被撤），order2 已全部成交
    assert.deepEqual(engine2.buildSnapshot().asks, []);
    // 序号水位恢复
    assert.equal(engine2.currentCommitSeq, 4);
    assert.equal(engine2.currentReceiveSeq, 3);

    // 重启后继续提交：commitSeq 接着增长，不会覆盖历史
    const cont = engine2.submit('s3', sell(98, 1));
    assert.equal(cont.ok, true);
    if (cont.ok) assert.equal(cont.orderId, 4);
    assert.equal(engine2.currentCommitSeq, 5);
    ledger2.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('拒绝回执同样持久化幂等：重启后同键仍返回同一拒绝，且不占提交号', () => {
  const dir = tempDir();
  const dbFile = join(dir, 'rej.db');
  try {
    const ledger = new SqliteLedger(dbFile);
    const engine = new Engine({ ledger });

    // 拒绝：未知订单取消（不占提交号）
    const r1 = engine.submit('rej-1', { kind: 'cancel', orderId: 42 });
    assert.equal(r1.ok, false);
    if (!r1.ok) assert.equal(r1.error.code, 'UNKNOWN_ORDER');
    assert.equal(engine.currentCommitSeq, 0);

    // 紧接着的接受命令占用 seq=1，证明拒绝没有占号
    const ok = engine.submit('ok-1', sell(100, 1));
    assert.equal(ok.ok, true);
    assert.equal(engine.currentCommitSeq, 1);

    // 同键同载荷：拿到同一个拒绝回执
    const r1Again = engine.submit('rej-1', { kind: 'cancel', orderId: 42 });
    assert.deepEqual(r1Again, r1);
    assert.equal(engine.currentCommitSeq, 1);
    ledger.close();

    // 重启后：拒绝键仍在（幂等表未随快照裁剪而删），命令簿只有 1 行接受
    const ledger2 = new SqliteLedger(dbFile);
    const engine2 = new Engine({ ledger: ledger2 });
    const { commits } = ledger2.replay();
    assert.equal(commits.length, 1);
    assert.equal(commits[0]!.requestKey, 'ok-1');
    assert.equal(engine2.currentCommitSeq, 1);

    const afterRestart = engine2.submit('rej-1', {
      kind: 'cancel',
      orderId: 42,
    });
    assert.equal(afterRestart.ok, false);
    if (!afterRestart.ok) {
      assert.equal(afterRestart.error.code, 'UNKNOWN_ORDER');
    }
    assert.equal(engine2.currentCommitSeq, 1); // 依旧不占号
    ledger2.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('快照裁剪后重启：从快照恢复；缺口订阅先发快照再追新提交', () => {
  const dir = tempDir();
  const dbFile = join(dir, 'c.db');
  try {
    const ledgerA = new SqliteLedger(dbFile);
    const engineA = new Engine({
      ledger: ledgerA,
      snapshotInterval: 3,
    });

    // 6 个提交：在 seq=3 与 seq=6 时各做一次快照并裁剪
    engineA.submit('s1', sell(100, 10));
    engineA.submit('s2', sell(101, 5));
    engineA.submit('b1', buy(100, 3, 'IOC')); // seq3 触发快照
    assert.equal(ledgerA.oldestCommitSeq(), 4);
    engineA.submit('s3', sell(99, 7));
    engineA.submit('b2', buy(99, 2, 'IOC'));
    engineA.submit('b3', buy(99, 9, 'IOC')); // seq6 触发快照
    assert.equal(ledgerA.oldestCommitSeq(), 7);
    const expectedBook = engineA.buildSnapshot();
    ledgerA.close();

    // 重启：事件已裁剪，靠快照恢复；发布器绑定在新引擎上
    const ledgerB = new SqliteLedger(dbFile);
    const pubB = new Publisher({
      getSnapshot: () => engineB.buildSnapshot(),
      getRetainedSnapshot: () => engineB.retainedSnapshot(),
      getOldestRetainedSeq: () => engineB.oldestRetainedSeq(),
      getHeadSeq: () => engineB.currentCommitSeq,
      readCommitsAfter: (s: number) => engineB.readCommitsAfter(s),
    });
    const engineB = new Engine({ ledger: ledgerB, publisher: pubB });
    const { snapshot, commits } = ledgerB.replay();
    assert.ok(snapshot);
    assert.equal(snapshot!.seq, 6);
    assert.equal(commits.length, 0);
    assert.deepEqual(engineB.buildSnapshot().asks, expectedBook.asks);
    assert.equal(engineB.currentCommitSeq, 6);

    // 订阅者从 seq=0 续读：缺口超出保留范围 -> 先发快照
    const frames: string[] = [];
    const transport = {
      send: (f: string) => frames.push(f),
      isOpen: () => true,
    };
    const ok = pubB.attach(transport, 0);
    assert.equal(ok, true);
    let parsed = frames.map((f) => JSON.parse(f));
    assert.equal(parsed.length, 1);
    assert.equal(parsed[0]!.type, 'snapshot');
    assert.equal(parsed[0]!.seq, 6);

    // 恢复后续提交，老订阅者实时收到新提交
    engineB.submit('s4', sell(98, 4));
    parsed = frames.map((f) => JSON.parse(f));
    assert.equal(parsed.length, 2);
    assert.equal(parsed[1]!.type, 'commits');
    assert.equal(parsed[1]!.fromSeq, 7);
    assert.equal(parsed[1]!.commits[0]!.seq, 7);

    // 新订阅者从 seq=5 续读：5 已被裁剪（oldest=7），先快照(seq6)再补 seq7
    const frames2: string[] = [];
    const transport2 = {
      send: (f: string) => frames2.push(f),
      isOpen: () => true,
    };
    pubB.attach(transport2, 5);
    const parsed2 = frames2.map((f) => JSON.parse(f));
    assert.equal(parsed2[0]!.type, 'snapshot');
    assert.equal(parsed2[0]!.seq, 6);
    assert.equal(parsed2[1]!.type, 'commits');
    assert.equal(parsed2[1]!.fromSeq, 7);

    // 从头序号不超前：lastSeq > head 拒绝
    const frames3: string[] = [];
    const ok3 = pubB.attach(
      { send: (f: string) => frames3.push(f), isOpen: () => true },
      999,
    );
    assert.equal(ok3, false);
    assert.equal(JSON.parse(frames3[0]!).type, 'replay_error');

    ledgerB.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
