import type { BookSnapshot, CommitRecord } from './types.js';

/** 传输抽象：真实 WebSocket 与测试假对象实现同一接口 */
export interface SubscriberTransport {
  send(frame: string): void;
  isOpen(): boolean;
}

export type SubscriberFrame =
  | { type: 'snapshot'; seq: number; snapshot: BookSnapshot }
  | {
      type: 'commits';
      fromSeq: number;
      toSeq: number;
      commits: Array<{
        seq: number;
        receiveSeq: number;
        requestKey: string;
        events: CommitRecord['events'];
      }>;
    }
  | {
      type: 'replay_error';
      code: 'AHEAD_OF_HEAD';
      message: string;
    };

interface Subscriber {
  transport: SubscriberTransport;
  /** 已确认投递到的提交序号；后续只推 > lastSeq 的提交 */
  lastSeq: number;
}

/**
 * 内存发布器。引擎在每条命令落盘成功后同步调用 publishCommitted，
 * 广播顺序与提交序号严格一致，因此订阅者看到的 seq 连续无跳号。
 *
 * 续读（attach lastSeq）：
 * - lastSeq < 账本最老保留序号：缺口已不可补发，先推当前快照，
 *   再推快照序号之后的全部提交；
 * - 缺口仍在保留范围：直接按序号补发提交；
 * - lastSeq > 当前头：拒绝（只能续读未来，不能倒读未来）。
 */
interface PublisherDeps {
  getSnapshot: () => BookSnapshot;
  getRetainedSnapshot: () => BookSnapshot | null;
  getOldestRetainedSeq: () => number;
  getHeadSeq: () => number;
  readCommitsAfter: (seq: number) => CommitRecord[];
}

export class Publisher {
  private subs = new Set<Subscriber>();
  private getSnapshot: () => BookSnapshot;
  private getRetainedSnapshot: () => BookSnapshot | null;
  private getOldest: () => number;
  private getHead: () => number;
  private readCommits: (fromSeq: number) => CommitRecord[];

  constructor(deps: PublisherDeps) {
    this.getSnapshot = deps.getSnapshot;
    this.getRetainedSnapshot = deps.getRetainedSnapshot;
    this.getOldest = deps.getOldestRetainedSeq;
    this.getHead = deps.getHeadSeq;
    this.readCommits = deps.readCommitsAfter;
  }

  /** 新增续读订阅；返回 false 表示 lastSeq 超前于当前头，无法订阅 */
  attach(transport: SubscriberTransport, lastSeq = 0): boolean {
    const head = this.getHead();
    if (lastSeq > head) {
      transport.send(
        JSON.stringify({
          type: 'replay_error',
          code: 'AHEAD_OF_HEAD',
          message: `lastSeq ${lastSeq} 超过当前头序号 ${head}`,
        } satisfies SubscriberFrame),
      );
      return false;
    }

    const sub: Subscriber = { transport, lastSeq };
    // 先入集合，保证 catchUp 期间到达的实时提交不会丢；
    // catchUp 内部会把订阅者读到与当前头一致。
    this.subs.add(sub);
    this.catchUp(sub);
    return true;
  }
  detach(transport: SubscriberTransport): void {
    for (const sub of this.subs) {
      if (sub.transport === transport) this.subs.delete(sub);
    }
  }

  subscriberCount(): number {
    return this.subs.size;
  }

  /** 引擎在 append 成功、内存推进之后调用（顺序与 commitSeq 一致） */
  publishCommitted(seq: number, record: CommitRecord): void {
    const frame = JSON.stringify({
      type: 'commits',
      fromSeq: seq,
      toSeq: seq,
      commits: [
        {
          seq: record.seq,
          receiveSeq: record.receiveSeq,
          requestKey: record.requestKey,
          events: record.events,
        },
      ],
    } satisfies SubscriberFrame);

    for (const sub of [...this.subs]) {
      if (!sub.transport.isOpen()) {
        this.subs.delete(sub);
        continue;
      }
      if (sub.lastSeq >= seq) continue; // 已读（理论上不发生）
      if (sub.lastSeq === seq - 1) {
        // 正常实时路径：无缺口，直接推
        sub.transport.send(frame);
        sub.lastSeq = seq;
      } else {
        // 落后于头（建连期间又有新提交）：走统一补发逻辑
        this.catchUp(sub);
      }
    }
  }

  /**
   * 建连/重连后的续读：缺口在保留范围内补发提交，超出范围先发快照。
   *
   * 关键：缺口超范围时必须发“持久化保留的快照”（其 seq 可能早于当前头），
   * 然后补发快照 seq 之后的全部提交；不能直接发当前内存态快照，否则与
   * 后续补发的提交序号对不上（快照已经隐含了这些提交）。
   */
  private catchUp(sub: Subscriber): void {
    const head = this.getHead();
    if (sub.lastSeq >= head) return;

    const oldest = this.getOldest();
    if (sub.lastSeq < oldest - 1) {
      const retained = this.getRetainedSnapshot();
      if (retained) {
        sub.transport.send(
          JSON.stringify({
            type: 'snapshot',
            seq: retained.seq,
            snapshot: retained,
          } satisfies SubscriberFrame),
        );
        sub.lastSeq = retained.seq;
      }
    }

    if (sub.lastSeq >= head) return;
    const commits = this.readCommits(sub.lastSeq);
    if (commits.length === 0) return;
    sub.transport.send(
      JSON.stringify({
        type: 'commits',
        fromSeq: commits[0]!.seq,
        toSeq: commits[commits.length - 1]!.seq,
        commits: commits.map((c) => ({
          seq: c.seq,
          receiveSeq: c.receiveSeq,
          requestKey: c.requestKey,
          events: c.events,
        })),
      } satisfies SubscriberFrame),
    );
    sub.lastSeq = commits[commits.length - 1]!.seq;
  }
}
