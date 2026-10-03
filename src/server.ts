import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { SqliteLedger } from './sqlite-ledger.js';
import { Engine } from './engine.js';
import { Publisher } from './publisher.js';
import {
  parseAmendBody,
  parseCancel,
  parsePlaceBody,
} from './protocol.js';
import { WebSocketConnection } from './websocket.js';
import type { OrderCommand, Receipt } from './types.js';

export interface AppServerOptions {
  dbPath: string;
  snapshotInterval?: number;
  port?: number;
}

export interface AppServer {
  port: number;
  engine: Engine;
  ledger: SqliteLedger;
  publisher: Publisher;
  close: () => void;
}

export function startServer(opts: AppServerOptions): AppServer {
  const ledger = new SqliteLedger(opts.dbPath);
  const publisher = new Publisher({
    getSnapshot: () => engine.buildSnapshot(),
    getRetainedSnapshot: () => engine.retainedSnapshot(),
    getOldestRetainedSeq: () => engine.oldestRetainedSeq(),
    getHeadSeq: () => engine.currentCommitSeq,
    readCommitsAfter: (seq) => engine.readCommitsAfter(seq),
  });
  const engine = new Engine({
    ledger,
    publisher,
    snapshotInterval: opts.snapshotInterval ?? 0,
  });

  const http = createServer((req, res) => handleHttp(req, res, engine));

  http.on('upgrade', (req, socket, head) => {
    const key = req.headers['sec-websocket-key'];
    if (
      req.headers.upgrade?.toLowerCase() !== 'websocket' ||
      typeof key !== 'string'
    ) {
      socket.destroy();
      return;
    }
    const conn = new WebSocketConnection(
      socket as unknown as import('node:net').Socket,
      key,
      head as Buffer | undefined,
    );
    // 首个文本消息必须是订阅请求：{"type":"subscribe","lastSeq":N}
    conn.onMessage = (text) => {
      let msg: { type?: string; lastSeq?: unknown };
      try {
        msg = JSON.parse(text) as { type?: string; lastSeq?: unknown };
      } catch {
        conn.send(JSON.stringify({ type: 'replay_error', code: 'BAD_MESSAGE' }));
        return;
      }
      if (msg.type !== 'subscribe') {
        conn.send(
          JSON.stringify({ type: 'replay_error', code: 'BAD_MESSAGE' }),
        );
        return;
      }
      const lastSeq =
        typeof msg.lastSeq === 'number' && Number.isInteger(msg.lastSeq)
          ? msg.lastSeq
          : 0;
      publisher.attach(conn, lastSeq);
    };
    conn.onClose = () => publisher.detach(conn);
  });

  const port = opts.port ?? 0;
  http.listen(port);
  const address = http.address() as AddressInfo;

  return {
    port: address.port,
    engine,
    ledger,
    publisher,
    close: () => {
      http.close();
      ledger.close();
    },
  };
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

async function handleHttp(
  req: IncomingMessage,
  res: ServerResponse,
  engine: Engine,
): Promise<void> {
  try {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const method = req.method ?? 'GET';

    if (method === 'GET' && url.pathname === '/health') {
      json(res, 200, {
        ok: true,
        commitSeq: engine.currentCommitSeq,
        receiveSeq: engine.currentReceiveSeq,
      });
      return;
    }

    if (method === 'GET' && url.pathname === '/snapshot') {
      json(res, 200, engine.buildSnapshot());
      return;
    }

    let command: OrderCommand | null = null;
    let parseError: string | null = null;

    if (method === 'POST' && url.pathname === '/orders') {
      const parsed = parsePlaceBody(await readJson(req));
      command = parsed.command ?? null;
      parseError = parsed.error ?? null;
    } else {
      const m = url.pathname.match(/^\/orders\/(\d+)\/(cancel|amend)$/);
      if (m && method === 'POST') {
        const [, id, action] = m;
        if (action === 'cancel') {
          const parsed = parseCancel(id);
          command = parsed.command ?? null;
          parseError = parsed.error ?? null;
        } else {
          const parsed = parseAmendBody(id, await readJson(req));
          command = parsed.command ?? null;
          parseError = parsed.error ?? null;
        }
      }
    }

    if (!command) {
      json(res, parseError ? 400 : 404, {
        ok: false,
        error: parseError ?? 'not found',
      });
      return;
    }

    const key =
      (req.headers['idempotency-key'] as string | undefined)?.trim() ?? '';
    if (!key) {
      json(res, 400, {
        ok: false,
        error: { code: 'INVALID_ARGUMENT', message: '缺少 Idempotency-Key 头' },
      } satisfies Receipt | { ok: false; error: unknown });
      return;
    }

    const receipt = engine.submit(key, command);
    json(res, receipt.ok ? 200 : receiptStatusCode(receipt), receipt);
  } catch (err) {
    json(res, 400, { ok: false, error: `请求处理失败：${(err as Error).message}` });
  }
}

function receiptStatusCode(r: Receipt): number {
  if (r.ok) return 200;
  switch (r.error.code) {
    case 'UNKNOWN_ORDER':
      return 404;
    case 'IDEMPOTENCY_KEY_CONFLICT':
      return 409;
    case 'LEDGER_UNAVAILABLE':
      return 503;
    default:
      return 400;
  }
}

function json(res: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(text),
  });
  res.end(text);
}

// 直接运行时启动服务
if (import.meta.url === `file://${process.argv[1]}`) {
  const server = startServer({
    dbPath: process.env.DB_PATH ?? './market.db',
    snapshotInterval: Number(process.env.SNAPSHOT_INTERVAL ?? 50),
    port: Number(process.env.PORT ?? 8080),
  });
  console.log(
    `sim-market listening on :${server.port} (db=${process.env.DB_PATH ?? './market.db'})`,
  );
}
