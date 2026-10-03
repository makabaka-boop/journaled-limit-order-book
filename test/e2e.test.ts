import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { connect, type Socket } from 'node:net';
import { startServer, type AppServer } from '../src/server.js';

function tempDir(): string {
  return mkdtempSync(join(tmpdir(), 'sim-market-e2e-'));
}

async function httpJson(
  port: number,
  method: string,
  path: string,
  body: unknown,
  idemKey?: string,
): Promise<{ status: number; json: any }> {
  const res = await fetch(`http://127.0.0.1:${port}${path}`, {
    method,
    headers: {
      'content-type': 'application/json',
      ...(idemKey ? { 'idempotency-key': idemKey } : {}),
    },
    body: method === 'GET' ? undefined : JSON.stringify(body),
  });
  return { status: res.status, json: await res.json() };
}

/** 原始 WebSocket 客户端：握手 + 订阅 + 收集服务端文本帧 */
class WsClient {
  private socket: Socket;
  private chunks: Buffer[] = [];
  private handshakeDone = false;
  private pending: Uint8Array = new Uint8Array(0);
  readonly received: any[] = [];
  private waiters: Array<{ count: number; resolve: (v: any[]) => void }> = [];

  constructor(port: number, lastSeq: number) {
    this.socket = connect(port, '127.0.0.1');
    const key = Buffer.from('test-websocket-key-123456789012').toString(
      'base64',
    );
    this.socket.on('connect', () => {
      this.socket.write(
        `GET /ws HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\n` +
          `Connection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\n` +
          `Sec-WebSocket-Version: 13\r\n\r\n`,
      );
      this.socket.write(
        encodeClientFrame(JSON.stringify({ type: 'subscribe', lastSeq })),
      );
    });
    this.socket.on('data', (d: Buffer) => this.feed(d));
    this.socket.on('error', () => {});
  }

  private feed(data: Buffer): void {
    if (!this.handshakeDone) {
      this.chunks.push(data);
      const all = Buffer.concat(this.chunks);
      const idx = all.indexOf('\r\n\r\n');
      if (idx < 0) return;
      this.handshakeDone = true;
      this.chunks = [];
      this.parseFrames(all.subarray(idx + 4));
      return;
    }
    this.parseFrames(data);
  }

  private parseFrames(initial: Buffer): void {
    const buf0: Buffer =
      this.pending.length > 0
        ? Buffer.concat([this.pending as Buffer, initial])
        : Buffer.from(initial);
    let buf: Buffer = buf0;
    this.pending = new Uint8Array(0);
    while (buf.length >= 2) {
      let len = buf[1]! & 0x7f;
      let off = 2;
      if (len === 126) {
        len = buf.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        len = Number(buf.readBigUInt64BE(2));
        off = 10;
      }
      if (buf.length < off + len) {
        this.pending = new Uint8Array(buf);
        return;
      }
      this.received.push(
        JSON.parse(buf.subarray(off, off + len).toString('utf8')),
      );
      buf = buf.subarray(off + len);
      this.pump();
    }
    if (buf.length > 0) this.pending = new Uint8Array(buf);
  }

  private pump(): void {
    this.waiters = this.waiters.filter((w) => {
      if (this.received.length >= w.count) {
        w.resolve(this.received.slice(0, w.count));
        return false;
      }
      return true;
    });
  }

  waitFor(count: number, timeoutMs = 2000): Promise<any[]> {
    if (this.received.length >= count) {
      return Promise.resolve(this.received.slice(0, count));
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`等待 ${count} 条 WS 消息超时（仅 ${this.received.length} 条）`)),
        timeoutMs,
      );
      this.waiters.push({
        count,
        resolve: (v) => {
          clearTimeout(timer);
          resolve(v);
        },
      });
    });
  }

  close(): void {
    this.socket.end();
  }
}

/** 构造一个带掩码的客户端文本帧 */
function encodeClientFrame(text: string): Buffer {
  const payload = Buffer.from(text, 'utf8');
  const len = payload.length;
  let header: Buffer;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[1] = 0x80 | len;
  } else {
    header = Buffer.alloc(4);
    header[1] = 0x80 | 126;
    header.writeUInt16BE(len, 2);
  }
  header[0] = 0x81;
  const mask = Buffer.from([1, 2, 3, 4]);
  const masked = Buffer.allocUnsafe(len);
  for (let i = 0; i < len; i++) masked[i] = payload[i]! ^ mask[i % 4]!;
  return Buffer.concat([header, mask, masked]);
}

test('端到端：HTTP 挂单/成交/取消/改单 + 幂等 + 409 + WS 实时订阅', async () => {
  const dir = tempDir();
  let server: AppServer | null = null;
  try {
    server = startServer({ dbPath: join(dir, 'e2e.db'), port: 0 });
    const port = server.port;

    // 无幂等键 -> 400
    const noKey = await httpJson(port, 'POST', '/orders', {
      side: 'SELL',
      price: 100,
      qty: 5,
    });
    assert.equal(noKey.status, 400);

    // 挂卖单（seq1）
    const r1 = await httpJson(
      port,
      'POST',
      '/orders',
      { side: 'SELL', price: 100, qty: 5, tif: 'GTC' },
      'e2e-sell1',
    );
    assert.equal(r1.status, 200);
    assert.equal(r1.json.ok, true);
    assert.equal(r1.json.orderId, 1);
    assert.equal(r1.json.restingQty, 5);

    // 连接 WS 订阅（从头），先补发 seq1
    const ws = new WsClient(port, 0);
    const firstBatch = await ws.waitFor(1);
    assert.equal(firstBatch[0]!.type, 'commits');
    assert.equal(firstBatch[0]!.fromSeq, 1);

    // 重复键 -> 原回执（无新帧）
    const dup = await httpJson(
      port,
      'POST',
      '/orders',
      { side: 'SELL', price: 100, qty: 5, tif: 'GTC' },
      'e2e-sell1',
    );
    assert.equal(dup.status, 200);
    assert.deepEqual(dup.json, r1.json);

    // 同键改载荷 -> 409
    const conflict = await httpJson(
      port,
      'POST',
      '/orders',
      { side: 'SELL', price: 99, qty: 5, tif: 'GTC' },
      'e2e-sell1',
    );
    assert.equal(conflict.status, 409);
    assert.equal(conflict.json.error.code, 'IDEMPOTENCY_KEY_CONFLICT');

    // 买单 IOC 立即成交 3（seq2）
    const buy = await httpJson(
      port,
      'POST',
      '/orders',
      { side: 'BUY', price: 100, qty: 3, tif: 'IOC' },
      'e2e-buy1',
    );
    assert.equal(buy.status, 200);
    assert.equal(buy.json.tradedQty, 3);
    assert.equal(buy.json.status, 'FILLED');
    assert.equal(buy.json.restingQty, 0);

    // 取消剩余卖单（seq3）
    const cancel = await httpJson(
      port,
      'POST',
      '/orders/1/cancel',
      {},
      'e2e-c1',
    );
    assert.equal(cancel.status, 200);
    assert.equal(cancel.json.status, 'CANCELLED');

    // 改单不存在 -> 404
    const amendMissing = await httpJson(
      port,
      'POST',
      '/orders/999/amend',
      { qty: 10 },
      'e2e-a1',
    );
    assert.equal(amendMissing.status, 404);

    // WS 实时收到 seq2(成交) 与 seq3(取消)
    const messages = await ws.waitFor(3);
    assert.equal(messages[1]!.fromSeq, 2);
    const seq2Events = messages[1]!.commits[0]!.events;
    assert.ok(
      seq2Events.some((e: any) => e.type === 'Trade'),
      'seq2 应包含 Trade 事件',
    );
    assert.equal(messages[2]!.fromSeq, 3);
    assert.equal(
      messages[2]!.commits[0]!.events[0]!.type,
      'OrderClosed',
    );
    ws.close();

    // 订单簿已空
    const snap = await httpJson(port, 'GET', '/snapshot', {});
    assert.deepEqual(snap.json.bids, []);
    assert.deepEqual(snap.json.asks, []);

    // 健康检查反映序号水位
    const health = await httpJson(port, 'GET', '/health', {});
    assert.equal(health.json.commitSeq, 3);
    assert.equal(health.json.receiveSeq, 2);
  } finally {
    server?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
