import { Matcher } from './matcher.js';
import type { Publisher } from './publisher.js';
import type {
  BookSnapshot,
  CommitRecord,
  DomainEvent,
  Ledger,
  OrderCommand,
  Receipt,
  Rejection,
} from './types.js';

export interface EngineOptions {
  ledger: Ledger;
  publisher?: Publisher;
  /** 每 N 个提交做一次快照并裁剪账本（0 = 不裁剪） */
  snapshotInterval?: number;
}

/**
 * 撮合器 / SQLite 账本 / 发布器围绕同一 commitSeq 推进的编排者。
 *
 * 不变量：
 * 1. append 成功才推进内存撮合状态、receiveSeq 水位，并向发布器广播；
 *    append 抛错则一切不推进，返回 LEDGER_UNAVAILABLE，客户端可原键重试。
 * 2. 重放只 apply 已落盘事件，不重新撮合、不重新发布，因此不可能
 *    “再次生成已存在的成交”。
 * 3. 相同请求键 + 相同载荷：直接返回历史回执，不占序号；
 *    相同请求键 + 不同载荷：拒绝（IDEMPOTENCY_KEY_CONFLICT）。
 */
export class Engine {
  readonly matcher = new Matcher();
  private readonly ledger: Ledger;
  private readonly publisher?: Publisher;
  private readonly snapshotInterval: number;
  private commitSeq = 0;
  private receiveSeq = 0;

  constructor(opts: EngineOptions) {
    this.ledger = opts.ledger;
    this.publisher = opts.publisher;
    this.snapshotInterval = opts.snapshotInterval ?? 0;
    this.replay();
  }

  private replay(): void {
    const { snapshot, commits } = this.ledger.replay();
    if (snapshot) {
      this.matcher.importState({ orders: snapshot.orders.map((o) => ({ ...o })) });
      this.commitSeq = snapshot.seq;
      this.receiveSeq = snapshot.receiveSeq;
    }
    for (const rec of commits) {
      // 仅重放事件流；Trade 事件以账本里的 tradeId 为准，不重新撮合。
      this.matcher.applyEvents(rec.events);
      this.commitSeq = rec.seq;
      if (rec.receiveSeq > this.receiveSeq) this.receiveSeq = rec.receiveSeq;
    }
  }

  get currentCommitSeq(): number {
    return this.commitSeq;
  }

  get currentReceiveSeq(): number {
    return this.receiveSeq;
  }

  /** 命令载荷指纹：键一致但指纹不同即冲突 */
  static payloadHash(command: OrderCommand): string {
    return stableStringify(command);
  }

  /**
   * 提交一条命令。
   * @returns 成功/失败回执；任何情况都不抛（磁盘故障转为 LEDGER_UNAVAILABLE）。
   */
  submit(requestKey: string, command: OrderCommand): Receipt {
    if (!requestKey) {
      return rejectionReceipt(requestKey, 0, {
        code: 'INVALID_ARGUMENT',
        message: '缺少幂等请求键',
      } satisfies Rejection);
    }
    const payloadHash = Engine.payloadHash(command);

    const existing = this.ledger.lookupRequest(requestKey, payloadHash);
    if (existing === 'CONFLICT') {
      return rejectionReceipt(requestKey, this.commitSeq, {
        code: 'IDEMPOTENCY_KEY_CONFLICT',
        message: '请求键已被不同的载荷使用，复用同一键不允许改变载荷',
      } satisfies Rejection);
    }
    if (existing) return existing;

    // 在候选接收序号上做纯计算（不改内存状态）。
    const candidateReceiveSeq = this.receiveSeq + 1;
    const processed = this.matcher.process(command, candidateReceiveSeq);

    // 被业务规则拒绝（未知单/非法参数）：不占提交序号、不发布、
    // 不改撮合状态；但把拒绝回执记入幂等表，同键重试仍拿到同一拒绝。
    if (!processed.accepted) {
      const rejection: Receipt = {
        ok: false,
        requestKey,
        receiveSeq: this.receiveSeq,
        error: processed.error,
      };
      try {
        this.ledger.append({
          seq: this.commitSeq, // 拒绝不占号：seq 仅作幂等行载体
          receiveSeq: this.receiveSeq,
          requestKey,
          command,
          events: [],
          receipt: rejection,
        });
      } catch (err) {
        return rejectionReceipt(requestKey, this.commitSeq, {
          code: 'LEDGER_UNAVAILABLE',
          message: `事件落盘失败：${(err as Error).message}`,
        } satisfies Rejection);
      }
      return rejection;
    }

    const consumeReceive = consumesReceiveSeq(command, processed.events);
    const assignedReceiveSeq = consumeReceive
      ? candidateReceiveSeq
      : this.receiveSeq;

    const receipt: Receipt = {
      ok: true,
      requestKey,
      receiveSeq: assignedReceiveSeq,
      events: processed.events,
      orderId: processed.result.orderId,
      status: processed.result.status,
      tradedQty: processed.result.tradedQty,
      restingQty: processed.result.restingQty,
    };

    const record: CommitRecord = {
      seq: this.commitSeq + 1,
      receiveSeq: assignedReceiveSeq,
      requestKey,
      command,
      events: processed.events,
      receipt,
    };

    try {
      this.ledger.append(record);
    } catch (err) {
      // 磁盘故障：内存状态、序号水位、发布器全部不动。
      return rejectionReceipt(requestKey, this.commitSeq, {
        code: 'LEDGER_UNAVAILABLE',
        message: `事件落盘失败：${(err as Error).message}`,
      } satisfies Rejection);
    }

    // 落盘成功后才推进、才广播（发布顺序与提交序号严格一致）。
    this.commitSeq = record.seq;
    if (consumeReceive) this.receiveSeq = candidateReceiveSeq;
    this.matcher.applyEvents(record.events);
    this.publisher?.publishCommitted(record.seq, record);

    this.maybeCompact();
    return receipt;
  }

  private maybeCompact(): void {
    if (this.snapshotInterval <= 0) return;
    if (this.commitSeq % this.snapshotInterval !== 0) return;
    const snap = this.buildSnapshot();
    try {
      this.ledger.compact(snap);
    } catch {
      // 快照裁剪属于维护动作，失败不影响已提交的事实与广播。
    }
  }

  buildSnapshot(): BookSnapshot {
    const s = this.matcher.snapshot();
    return {
      seq: this.commitSeq,
      receiveSeq: this.receiveSeq,
      orders: s.orders.map((o) => ({ ...o })),
      bids: s.bids,
      asks: s.asks,
    };
  }

  /** 最近一次持久化快照（缺口恢复必须用它，而不是当前内存态） */
  retainedSnapshot(): BookSnapshot | null {
    return this.ledger.loadRetainedSnapshot();
  }

  /** 订阅器判定历史缺口时查询账本保留水位 */
  oldestRetainedSeq(): number {
    return this.ledger.oldestCommitSeq();
  }

  /** 缺口恢复用：读取某序号之后仍保留的全部提交（含事件，供补发） */
  readCommitsAfter(seq: number): CommitRecord[] {
    if (this.commitSeq <= seq) return [];
    return this.ledger.readRange(seq + 1, this.commitSeq);
  }
}

/** 只有“新挂单”以及“改价/加量导致重排队”才消耗接收序号 */
function consumesReceiveSeq(
  cmd: OrderCommand,
  events: DomainEvent[],
): boolean {
  if (cmd.kind === 'place') return true;
  if (cmd.kind === 'amend') {
    return events.some(
      (e) => e.type === 'OrderAmended' && e.requeued,
    );
  }
  return false;
}

function rejectionReceipt(
  requestKey: string,
  receiveSeq: number,
  error: Rejection,
): Receipt {
  return { ok: false, requestKey, receiveSeq, error };
}

/** 确定性 JSON：对象键排序，保证同一载荷指纹稳定 */
export function stableStringify(value: unknown): string {
  return JSON.stringify(sortValue(value));
}

function sortValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortValue);
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      out[key] = sortValue((value as Record<string, unknown>)[key]);
    }
    return out;
  }
  return value;
}
