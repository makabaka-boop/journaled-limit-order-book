import type {
  DomainEvent,
  Order,
  OrderCommand,
  OrderStatus,
  Rejection,
  Side,
} from './types.js';

export interface SuccessResult {
  /** 命令主订单：下单为新单，取消/改单为目标订单 */
  orderId: number;
  status: OrderStatus | 'CANCELLED';
  /** 本次命令成交数量 */
  tradedQty: number;
  /** 命令结束后主订单的挂单剩余量（未存活为 0） */
  restingQty: number;
}

export type ProcessResult =
  | { accepted: true; events: DomainEvent[]; result: SuccessResult }
  | { accepted: false; error: Rejection };

/**
 * 纯内存撮合器，无 IO、无序号分配。
 *
 * 唯一的状态推进方式是 applyEvent()：
 * - 命令处理在当前状态的克隆工作态上，边“生成事件”边 applyEvent 推进；
 * - 真正提交时，引擎对本撮合器重放同一批事件；
 * - 重启重放也只重放事件。
 * 三条路径共用同一份状态机代码，避免实时与重放语义漂移。
 *
 * 成交优先级：价格优先（买高卖低），同价按 prioritySeq（接收序号）小者优先。
 * 改单改价或加量 -> requeued，分配新接收序号，排到同价位队尾。
 */
export class Matcher {
  private orders = new Map<number, Order>();
  private books: Record<Side, Map<number, Order[]>> = {
    BUY: new Map(),
    SELL: new Map(),
  };

  // ---------- 命令校验 ----------

  static validateCommand(cmd: OrderCommand): Rejection | null {
    if (cmd.kind === 'place') {
      if (
        !Number.isInteger(cmd.price) ||
        cmd.price <= 0 ||
        !Number.isInteger(cmd.qty) ||
        cmd.qty <= 0
      ) {
        return { code: 'INVALID_ARGUMENT', message: 'price 与 qty 必须为正整数' };
      }
      if (cmd.tif !== 'GTC' && cmd.tif !== 'IOC') {
        return { code: 'INVALID_ARGUMENT', message: `未知 TIF: ${cmd.tif}` };
      }
      return null;
    }
    if (cmd.kind === 'cancel') {
      if (!Number.isInteger(cmd.orderId) || cmd.orderId <= 0) {
        return { code: 'INVALID_ARGUMENT', message: 'orderId 非法' };
      }
      return null;
    }
    if (!Number.isInteger(cmd.orderId) || cmd.orderId <= 0) {
      return { code: 'INVALID_ARGUMENT', message: 'orderId 非法' };
    }
    if (cmd.price === undefined && cmd.qty === undefined) {
      return {
        code: 'INVALID_ARGUMENT',
        message: '改单至少需要提供 price 或 qty',
      };
    }
    if (cmd.price !== undefined && (!Number.isInteger(cmd.price) || cmd.price <= 0)) {
      return { code: 'INVALID_ARGUMENT', message: 'price 必须为正整数' };
    }
    if (cmd.qty !== undefined && (!Number.isInteger(cmd.qty) || cmd.qty <= 0)) {
      return { code: 'INVALID_ARGUMENT', message: 'qty 必须为正整数' };
    }
    return null;
  }

  // ---------- 命令处理（纯函数：不改本对象状态） ----------

  /**
   * @param receiveSeq 引擎预留的接收序号：
   *   place 时即新订单 id/排队号；amend 改价或加量时为重排队的新排队号；
   *   其他情况不消耗。
   */
  process(cmd: OrderCommand, receiveSeq: number): ProcessResult {
    const validationError = Matcher.validateCommand(cmd);
    if (validationError) return { accepted: false, error: validationError };

    // 在克隆工作态上推演；本对象的真实状态只允许 applyEvents 推进。
    const w = new Matcher();
    w.importState(this.exportState());

    if (cmd.kind === 'place') return w.handlePlace(cmd, receiveSeq);
    if (cmd.kind === 'cancel') return w.handleCancel(cmd);
    return w.handleAmend(cmd, receiveSeq);
  }

  private handlePlace(
    cmd: Extract<OrderCommand, { kind: 'place' }>,
    receiveSeq: number,
  ): ProcessResult {
    const accepted: DomainEvent = {
      type: 'OrderAccepted',
      orderId: receiveSeq,
      side: cmd.side,
      price: cmd.price,
      qty: cmd.qty,
      tif: cmd.tif,
    };
    this.applyEvent(accepted);
    const events: DomainEvent[] = [accepted];
    this.matchTaker(receiveSeq, cmd.price, cmd.qty, cmd.tif, receiveSeq, events);
    return {
      accepted: true,
      events,
      result: this.resultFor(receiveSeq, events),
    };
  }

  private handleCancel(
    cmd: Extract<OrderCommand, { kind: 'cancel' }>,
  ): ProcessResult {
    const o = this.orders.get(cmd.orderId);
    if (!o || o.status !== 'LIVE') {
      return {
        accepted: false,
        error: {
          code: 'UNKNOWN_ORDER',
          message: `订单 ${cmd.orderId} 不存在或已终结`,
        },
      };
    }
    const ev: DomainEvent = {
      type: 'OrderClosed',
      orderId: o.id,
      reason: 'CANCELLED',
      remainingQty: o.qty - o.filledQty,
    };
    this.applyEvent(ev);
    return {
      accepted: true,
      events: [ev],
      result: this.resultFor(o.id, [ev]),
    };
  }

  private handleAmend(
    cmd: Extract<OrderCommand, { kind: 'amend' }>,
    receiveSeq: number,
  ): ProcessResult {
    const o = this.orders.get(cmd.orderId);
    if (!o || o.status !== 'LIVE') {
      return {
        accepted: false,
        error: {
          code: 'UNKNOWN_ORDER',
          message: `订单 ${cmd.orderId} 不存在或已终结`,
        },
      };
    }
    const newPrice = cmd.price ?? o.price;
    const newQty = cmd.qty ?? o.qty;
    if (newQty <= o.filledQty) {
      return {
        accepted: false,
        error: {
          code: 'INVALID_ARGUMENT',
          message: `改单数量 ${newQty} 必须大于已成交数量 ${o.filledQty}`,
        },
      };
    }
    const requeued = newPrice !== o.price || newQty > o.qty;
    const newPriority = requeued ? receiveSeq : o.prioritySeq;
    const amended: DomainEvent = {
      type: 'OrderAmended',
      orderId: o.id,
      price: newPrice,
      qty: newQty,
      requeued,
      newPrioritySeq: newPriority,
    };
    this.applyEvent(amended);
    const events: DomainEvent[] = [amended];

    if (requeued) {
      // 重新挂入后立即以新价格参与撮合（接收序号为新序号，排同价位队尾）。
      this.matchTaker(o.id, newPrice, newQty, 'GTC', receiveSeq, events);
    }
    return {
      accepted: true,
      events,
      result: this.resultFor(o.id, events),
    };
  }

  /**
   * 让工作态中的 taker 吃对手盘。每个状态变化都先入事件、再 applyEvent，
   * 保证事件流是状态变化的唯一描述。
   */
  private matchTaker(
    takerId: number,
    limitPrice: number,
    targetQty: number,
    tif: Order['tif'],
    tradeSeq: number,
    events: DomainEvent[],
  ): void {
    const taker = this.orders.get(takerId)!;
    const opposite: Side = taker.side === 'BUY' ? 'SELL' : 'BUY';
    let tradeNo = 0;

    while (taker.filledQty < targetQty) {
      const level = this.bestLevel(opposite);
      if (!level) break;
      const maker = level[0]!;
      const crosses =
        taker.side === 'BUY'
          ? maker.price <= limitPrice
          : maker.price >= limitPrice;
      if (!crosses) break;

      const takerRemaining = targetQty - taker.filledQty;
      const makerRemaining = maker.qty - maker.filledQty;
      const tradeQty = Math.min(takerRemaining, makerRemaining);
      tradeNo += 1;

      const trade: DomainEvent = {
        type: 'Trade',
        tradeId: `T${tradeSeq}-${tradeNo}`,
        price: maker.price,
        qty: tradeQty,
        takerSide: taker.side,
        makerOrderId: maker.id,
        takerOrderId: taker.id,
      };
      this.applyEvent(trade);
      events.push(trade);

      if (maker.filledQty >= maker.qty) {
        const makerClosed: DomainEvent = {
          type: 'OrderClosed',
          orderId: maker.id,
          reason: 'FILLED',
          remainingQty: 0,
        };
        this.applyEvent(makerClosed);
        events.push(makerClosed);
      }
    }

    const remaining = targetQty - taker.filledQty;
    let finish: DomainEvent;
    if (remaining === 0) {
      finish = {
        type: 'OrderClosed',
        orderId: taker.id,
        reason: 'FILLED',
        remainingQty: 0,
      };
    } else if (tif === 'IOC') {
      finish = {
        type: 'OrderClosed',
        orderId: taker.id,
        reason: 'EXPIRED',
        remainingQty: remaining,
      };
    } else {
      finish = {
        type: 'OrderResting',
        orderId: taker.id,
        remainingQty: remaining,
      };
    }
    this.applyEvent(finish);
    events.push(finish);
  }

  private resultFor(orderId: number, events: DomainEvent[]): SuccessResult {
    const o = this.orders.get(orderId)!;
    let tradedQty = 0;
    for (const e of events) {
      if (e.type === 'Trade' && e.takerOrderId === orderId) {
        tradedQty += e.qty;
      }
    }
    return {
      orderId,
      status: o.status,
      tradedQty,
      restingQty: o.status === 'LIVE' ? o.qty - o.filledQty : 0,
    };
  }

  // ---------- 事件状态机：实时提交 / 重放 / 工作态推演共用 ----------

  applyEvents(events: DomainEvent[]): void {
    for (const e of events) this.applyEvent(e);
  }

  private applyEvent(e: DomainEvent): void {
    switch (e.type) {
      case 'OrderAccepted': {
        const o: Order = {
          id: e.orderId,
          side: e.side,
          price: e.price,
          qty: e.qty,
          filledQty: 0,
          tif: e.tif,
          prioritySeq: e.orderId,
          status: 'LIVE',
        };
        this.orders.set(o.id, o);
        break;
      }
      case 'Trade': {
        this.orders.get(e.makerOrderId)!.filledQty += e.qty;
        this.orders.get(e.takerOrderId)!.filledQty += e.qty;
        break;
      }
      case 'OrderClosed': {
        const o = this.orders.get(e.orderId)!;
        if (o.status === 'LIVE') this.removeFromBook(o);
        o.status =
          e.reason === 'CANCELLED'
            ? 'CANCELLED'
            : e.reason === 'EXPIRED'
              ? 'EXPIRED'
              : 'FILLED';
        break;
      }
      case 'OrderResting': {
        const o = this.orders.get(e.orderId)!;
        o.status = 'LIVE';
        this.insertIntoBook(o);
        break;
      }
      case 'OrderAmended': {
        const o = this.orders.get(e.orderId)!;
        if (o.status === 'LIVE') this.removeFromBook(o);
        o.price = e.price;
        o.qty = e.qty;
        o.prioritySeq = e.newPrioritySeq;
        if (!e.requeued) {
          // 仅减（或不变）量：prioritySeq 未变，插回后保持原排队位置。
          this.insertIntoBook(o);
        }
        // requeued 时暂不回簿：后续 Trade / OrderResting / OrderClosed
        // 与新挂单走完全相同的路径。
        break;
      }
    }
  }

  // ---------- 订单簿容器操作 ----------

  private removeFromBook(o: Order): void {
    const levels = this.books[o.side];
    const level = levels.get(o.price);
    if (!level) return;
    const idx = level.findIndex((x) => x.id === o.id);
    if (idx >= 0) level.splice(idx, 1);
    if (level.length === 0) levels.delete(o.price);
  }

  private insertIntoBook(o: Order): void {
    const levels = this.books[o.side];
    let level = levels.get(o.price);
    if (!level) {
      level = [];
      levels.set(o.price, level);
    }
    let i = 0;
    while (i < level.length && level[i]!.prioritySeq <= o.prioritySeq) i++;
    level.splice(i, 0, o);
  }

  private bestLevel(side: Side): Order[] | null {
    const levels = this.books[side];
    if (levels.size === 0) return null;
    const prices = [...levels.keys()];
    const best = side === 'BUY' ? Math.max(...prices) : Math.min(...prices);
    return levels.get(best)!;
  }

  // ---------- 快照 / 状态导入导出 ----------

  exportState(): MatcherState {
    return { orders: [...this.orders.values()].map((o) => ({ ...o })) };
  }

  importState(state: MatcherState): void {
    this.books = { BUY: new Map(), SELL: new Map() };
    this.orders = new Map();
    for (const saved of state.orders) {
      const o: Order = { ...saved };
      this.orders.set(o.id, o);
      if (o.status === 'LIVE') this.insertIntoBook(o);
    }
  }

  snapshot(): {
    orders: Order[];
    bids: Array<{ price: number; qty: number }>;
    asks: Array<{ price: number; qty: number }>;
  } {
    const summarize = (side: Side) =>
      [...this.books[side].entries()]
        .map(([price, level]) => ({
          price,
          qty: level.reduce((s, o) => s + (o.qty - o.filledQty), 0),
        }))
        .sort((a, b) => (side === 'BUY' ? b.price - a.price : a.price - b.price));
    return {
      orders: this.exportState().orders,
      bids: summarize('BUY'),
      asks: summarize('SELL'),
    };
  }

  /** 测试/调试用最优档 */
  topOfBook(side: Side): { price: number; qty: number } | null {
    const level = this.bestLevel(side);
    if (!level) return null;
    return {
      price: level[0]!.price,
      qty: level.reduce((s, o) => s + (o.qty - o.filledQty), 0),
    };
  }

  getOrder(id: number): Order | undefined {
    return this.orders.get(id);
  }
}

export interface MatcherState {
  orders: Order[];
}
