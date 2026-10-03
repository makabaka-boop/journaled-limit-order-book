/**
 * 全局共享类型：命令、事件、回执、快照。
 *
 * 序号约定：
 * - receiveSeq：服务端接收序号，单调递增，挂单成功落盘时分配，
 *   同时充当订单号与价格优先相同时的排队优先级（值小者优先）。
 * - commitSeq：提交序号，每个成功落盘的命令事务占用一个号；
 *   撮合器状态、SQLite 账本、WebSocket 发布器全部围绕它推进。
 */

export type Side = 'BUY' | 'SELL';
export type Tif = 'GTC' | 'IOC';
export type OrderStatus = 'LIVE' | 'FILLED' | 'CANCELLED' | 'EXPIRED';

/** 一条限价挂单在撮合器中的完整状态 */
export interface Order {
  id: number; // 等于下单成功时的 receiveSeq
  side: Side;
  price: number;
  /** 原始数量（改单后为改单目标总数量） */
  qty: number;
  /** 已成交数量 */
  filledQty: number;
  tif: Tif;
  /** 当前排队序号；改价或加量会被分配新的 receiveSeq 而排到队尾 */
  prioritySeq: number;
  status: OrderStatus;
}

export type OrderCommand =
  | { kind: 'place'; side: Side; price: number; qty: number; tif: Tif }
  | { kind: 'cancel'; orderId: number }
  | { kind: 'amend'; orderId: number; price?: number; qty?: number };

/** 单个命令处理后产生的领域事件序列（一个事务内按顺序落盘/发布/回放） */
export type DomainEvent =
  | {
      type: 'OrderAccepted';
      orderId: number;
      side: Side;
      price: number;
      qty: number;
      tif: Tif;
    }
  | {
      type: 'OrderResting';
      orderId: number;
      remainingQty: number;
    }
  | {
      type: 'Trade';
      tradeId: string;
      price: number;
      qty: number;
      takerSide: Side;
      makerOrderId: number;
      takerOrderId: number;
    }
  | {
      type: 'OrderClosed';
      orderId: number;
      reason: 'FILLED' | 'CANCELLED' | 'EXPIRED';
      remainingQty: number;
    }
  | {
      type: 'OrderAmended';
      orderId: number;
      price: number;
      qty: number; // 改单后的目标总数量
      requeued: boolean;
      newPrioritySeq: number;
    };

export interface Rejection {
  code:
    | 'UNKNOWN_ORDER'
    | 'INVALID_ARGUMENT'
    | 'IDEMPOTENCY_KEY_CONFLICT'
    | 'LEDGER_UNAVAILABLE';
  message: string;
}

/** 命令回执：成功或拒绝均带原请求键，作为重复请求的返回体 */
export type Receipt =
  | ({
      ok: true;
      requestKey: string;
      receiveSeq: number;
      events: DomainEvent[];
    } & OrderResult)
  | {
      ok: false;
      requestKey: string;
      receiveSeq: number;
      error: Rejection;
    };

export interface OrderResult {
  /** 本次命令关联的订单号（取消/改单为目标订单；下单为新订单；纯校验拒绝为 0） */
  orderId: number;
  status: OrderStatus | 'REJECTED';
  tradedQty: number;
  restingQty: number;
}

/** 账本中一条已提交记录：提交序号 + 命令 + 事件 + 回执哈希 */
export interface CommitRecord {
  seq: number;
  receiveSeq: number;
  requestKey: string;
  command: OrderCommand;
  events: DomainEvent[];
  receipt: Receipt;
}

/** 订阅续读所需的订单簿快照 */
export interface BookSnapshot {
  seq: number; // 制作快照时的 commitSeq
  receiveSeq: number;
  orders: Array<{
    id: number;
    side: Side;
    price: number;
    qty: number;
    filledQty: number;
    tif: Tif;
    prioritySeq: number;
    status: OrderStatus;
  }>;
  bids: Array<{ price: number; qty: number }>;
  asks: Array<{ price: number; qty: number }>;
}

export interface Ledger {
  /**
   * 原子化落盘一条命令：
   * - 幂等回执（成功或被拒）总是写入 requests 表，保证同键重放一致；
   * - 只有“被接受、真正推进账本”的命令才写 commits 行并占用 commitSeq；
   * - 失败必须抛出，调用方保证内存状态不推进、不发布。
   */
  append(rec: CommitRecord): void;
  /** 重启重放：快照（若有）之后的全部已提交记录，按序号升序 */
  replay(): { snapshot: BookSnapshot | null; commits: CommitRecord[] };
  /** 当前持久化的提交序号（账本水位） */
  lastCommitSeq(): number;
  /** 当前持久化的接收序号水位 */
  lastReceiveSeq(): number;
  /** 幂等键：返回历史持久化回执；键存在但载荷不同返回冲突 */
  lookupRequest(key: string, payloadHash: string): Receipt | 'CONFLICT' | null;
  /** 保存快照并裁剪早于快照的提交记录（幂等表不裁剪） */
  compact(snapshot: BookSnapshot): void;
  /** 读取最近一次持久化的快照（可能落后于当前头；供缺口恢复） */
  loadRetainedSnapshot(): BookSnapshot | null;
  oldestCommitSeq(): number;
  /** 按序号区间读取提交记录（订阅补发用，两端闭区间） */
  readRange(fromSeq: number, toSeq: number): CommitRecord[];
  close(): void;
}
