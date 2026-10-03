import Database from 'better-sqlite3';
import type { Database as DB } from 'better-sqlite3';
import { stableStringify } from './engine.js';
import type {
  BookSnapshot,
  CommitRecord,
  Ledger,
  OrderCommand,
  Receipt,
} from './types.js';

/** 简化预处理语句接口（兼容 @types/better-sqlite3 各版本） */
interface Stmt {
  run(...params: unknown[]): unknown;
  get(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
}

/**
 * SQLite 事件账本。
 *
 * 表设计：
 * - commits：每个成功提交的命令事务一行（序号、载荷、事件、回执）；
 * - requests：幂等键 -> 回执（不随快照裁剪而删除，保证重启后仍能去重）；
 * - snapshots：仅保留最新一份快照；
 * - meta：账本元信息（当前持久化水位，供裁剪后查询）。
 *
 * 所有写操作都在单个 sqlite 事务里完成：commit 行与 request 行要么都在，
 * 要么都不在，从根上避免“发了回执但没记账”。
 */
export class SqliteLedger implements Ledger {
  private readonly db: DB;
  private fault: Error | null = null;

  private readonly stmts: {
    insertCommit: Stmt;
    insertRequest: Stmt;
    selectRequest: Stmt;
    selectCommits: Stmt;
    selectRange: Stmt;
    maxCommit: Stmt;
    maxReceive: Stmt;
    minCommit: Stmt;
    upsertSnapshot: Stmt;
    selectSnapshot: Stmt;
    deleteOldCommits: Stmt;
    setMeta: Stmt;
  };

  constructor(filename: string) {
    this.db = new Database(filename);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('foreign_keys = ON');
    this.db.exec(SCHEMA);

    const p = (sql: string): Stmt => this.db.prepare(sql) as Stmt;
    this.stmts = {
      insertCommit: p(
        `INSERT INTO commits(seq, receive_seq, request_key, command, events, receipt)
         VALUES (@seq, @receiveSeq, @requestKey, @command, @events, @receipt)`,
      ),
      insertRequest: p(
        `INSERT INTO requests(request_key, payload_hash, receipt)
         VALUES (@key, @hash, @receipt)`,
      ),
      selectRequest: p(
        `SELECT payload_hash AS hash, receipt FROM requests WHERE request_key = ?`,
      ),
      selectCommits: p(
        `SELECT seq, receive_seq AS receiveSeq, request_key AS requestKey,
                command, events, receipt
         FROM commits WHERE seq > ? ORDER BY seq ASC`,
      ),
      selectRange: p(
        `SELECT seq, receive_seq AS receiveSeq, request_key AS requestKey,
                command, events, receipt
         FROM commits WHERE seq BETWEEN ? AND ? ORDER BY seq ASC`,
      ),
      maxCommit: p(`SELECT COALESCE(MAX(seq), 0) AS v FROM commits`),
      maxReceive: p(`SELECT COALESCE(MAX(receive_seq), 0) AS v FROM commits`),
      minCommit: p(`SELECT COALESCE(MIN(seq), 0) AS v FROM commits`),
      upsertSnapshot: p(
        `INSERT INTO snapshots(id, seq, data) VALUES (1, @seq, @data)
         ON CONFLICT(id) DO UPDATE SET seq = @seq, data = @data`,
      ),
      selectSnapshot: p(`SELECT seq, data FROM snapshots WHERE id = 1`),
      deleteOldCommits: p(`DELETE FROM commits WHERE seq <= ?`),
      setMeta: p(
        `INSERT INTO meta(key, value) VALUES (@k, @v)
         ON CONFLICT(key) DO UPDATE SET value = @v`,
      ),
    };
  }

  /** 注入一次/持续的落盘故障：append 会抛错且什么都不写 */
  injectFault(err = new Error('injected disk failure')): void {
    this.fault = err;
  }

  clearFault(): void {
    this.fault = null;
  }

  append(rec: CommitRecord): void {
    if (this.fault) throw this.fault;

    const tx = this.db.transaction((r: CommitRecord) => {
      // 只有真正推进账本的接受命令才写 commits 行并占用提交序号；
      // 业务拒绝只落幂等表（见 Ledger.append 契约）。
      const accepted = r.receipt.ok;
      if (accepted) {
        this.stmts.insertCommit.run({
          seq: r.seq,
          receiveSeq: r.receiveSeq,
          requestKey: r.requestKey,
          command: stableStringify(r.command),
          events: JSON.stringify(r.events),
          receipt: JSON.stringify(r.receipt),
        });
        this.stmts.setMeta.run({
          k: 'lastCommitSeq',
          v: String(r.seq),
        });
        this.stmts.setMeta.run({
          k: 'lastReceiveSeq',
          v: String(r.receiveSeq),
        });
      }
      // 幂等回执（成功或拒绝）写 requests 表；与可能的 commit 行同事务。
      this.stmts.insertRequest.run({
        key: r.requestKey,
        hash: stableStringify(r.command),
        receipt: JSON.stringify(r.receipt),
      });
    });
    tx(rec);
  }

  replay(): { snapshot: BookSnapshot | null; commits: CommitRecord[] } {
    const snapRow = this.stmts.selectSnapshot.get() as
      | { seq: number; data: string }
      | undefined;
    const snapshot = snapRow
      ? (JSON.parse(snapRow.data) as BookSnapshot)
      : null;
    const fromSeq = snapshot?.seq ?? 0;
    const rows = this.stmts.selectCommits.all(fromSeq) as Array<{
      seq: number;
      receiveSeq: number;
      requestKey: string;
      command: string;
      events: string;
      receipt: string;
    }>;
    return {
      snapshot,
      commits: rows.map((r) => ({
        seq: r.seq,
        receiveSeq: r.receiveSeq,
        requestKey: r.requestKey,
        command: JSON.parse(r.command) as OrderCommand,
        events: JSON.parse(r.events) as CommitRecord['events'],
        receipt: JSON.parse(r.receipt) as Receipt,
      })),
    };
  }

  lastCommitSeq(): number {
    const snap = this.stmts.selectSnapshot.get() as { seq: number } | undefined;
    const max = (this.stmts.maxCommit.get() as { v: number }).v;
    return Math.max(snap?.seq ?? 0, max);
  }

  lastReceiveSeq(): number {
    const snap = this.stmts.selectSnapshot.get() as
      | { data: string }
      | undefined;
    const snapReceive = snap
      ? (JSON.parse(snap.data) as BookSnapshot).receiveSeq
      : 0;
    const max = (this.stmts.maxReceive.get() as { v: number }).v;
    return Math.max(snapReceive, max);
  }

  lookupRequest(key: string, payloadHash: string): Receipt | 'CONFLICT' | null {
    const row = this.stmts.selectRequest.get(key) as
      | { hash: string; receipt: string }
      | undefined;
    if (!row) return null;
    if (row.hash !== payloadHash) return 'CONFLICT';
    return JSON.parse(row.receipt) as Receipt;
  }

  compact(snapshot: BookSnapshot): void {
    if (this.fault) throw this.fault;
    const tx = this.db.transaction((s: BookSnapshot) => {
      this.stmts.upsertSnapshot.run({
        seq: s.seq,
        data: JSON.stringify(s),
      });
      this.stmts.deleteOldCommits.run(s.seq);
      this.stmts.setMeta.run({
        k: 'oldestCommitSeq',
        v: String(s.seq + 1),
      });
    });
    tx(snapshot);
  }

  oldestCommitSeq(): number {
    const snap = this.stmts.selectSnapshot.get() as { seq: number } | undefined;
    const min = (this.stmts.minCommit.get() as { v: number }).v;
    if (!snap) return min === 0 ? 1 : min;
    // 快照覆盖到 snap.seq；可读事件从 snap.seq+1 起（若已全部裁掉）。
    return min === 0 ? snap.seq + 1 : min;
  }

  loadRetainedSnapshot(): BookSnapshot | null {
    const row = this.stmts.selectSnapshot.get() as
      | { data: string }
      | undefined;
    return row ? (JSON.parse(row.data) as BookSnapshot) : null;
  }

  readRange(fromSeq: number, toSeq: number): CommitRecord[] {
    const rows = this.stmts.selectRange.all(fromSeq, toSeq) as Array<{
      seq: number;
      receiveSeq: number;
      requestKey: string;
      command: string;
      events: string;
      receipt: string;
    }>;
    return rows.map((r) => ({
      seq: r.seq,
      receiveSeq: r.receiveSeq,
      requestKey: r.requestKey,
      command: JSON.parse(r.command) as OrderCommand,
      events: JSON.parse(r.events) as CommitRecord['events'],
      receipt: JSON.parse(r.receipt) as Receipt,
    }));
  }

  close(): void {
    this.db.close();
  }
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS commits (
  seq         INTEGER PRIMARY KEY,
  receive_seq INTEGER NOT NULL,
  request_key TEXT NOT NULL,
  command     TEXT NOT NULL,
  events      TEXT NOT NULL,
  receipt     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS requests (
  request_key  TEXT PRIMARY KEY,
  payload_hash TEXT NOT NULL,
  receipt      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
  id   INTEGER PRIMARY KEY CHECK (id = 1),
  seq  INTEGER NOT NULL,
  data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`;
