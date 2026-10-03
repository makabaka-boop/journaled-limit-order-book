/**
 * 独立小型撮合模型（测试专用 oracle）。
 *
 * 与 src/matcher.ts 刻意不共享任何实现：用最朴素的数组/排序表达同一套规则，
 * 然后在差分测试里把相同命令流分别喂给“真引擎（SQLite+事件重放）”与本模型，
 * 比对订单簿剩余量、成交（价格/数量/对手方）、订单终态是否逐笔一致。
 */

export interface MiniOrder {
  id: number;
  side: 'BUY' | 'SELL';
  price: number;
  qty: number; // 目标总量（改单后更新）
  filled: number;
  tif: 'GTC' | 'IOC';
  seq: number; // 当前排队序号
  status: 'LIVE' | 'FILLED' | 'CANCELLED' | 'EXPIRED';
}

export interface MiniTrade {
  tradeId: string;
  price: number;
  qty: number;
  makerId: number;
  takerId: number;
}

export interface MiniOutcome {
  accepted: boolean;
  /** 本次命令是否消耗了新的接收序号 */
  consumeReceive: boolean;
  trades: MiniTrade[];
  /** 主订单 id；纯校验拒绝为 0 */
  orderId: number;
  status: MiniOrder['status'] | 'REJECTED';
  tradedQty: number;
  restingQty: number;
  errorCode?: string;
}

export class MiniMatcher {
  private orders = new Map<number, MiniOrder>();
  private resting: MiniOrder[] = []; // 仅存活挂单

  place(
    id: number,
    side: 'BUY' | 'SELL',
    price: number,
    qty: number,
    tif: 'GTC' | 'IOC',
  ): MiniOutcome {
    if (!Number.isInteger(price) || price <= 0 || !Number.isInteger(qty) || qty <= 0) {
      return this.rejected('INVALID_ARGUMENT');
    }
    const o: MiniOrder = {
      id,
      side,
      price,
      qty,
      filled: 0,
      tif,
      seq: id,
      status: 'LIVE',
    };
    this.orders.set(id, o);
    const trades = this.cross(o, id);
    if (o.filled === o.qty) {
      o.status = 'FILLED';
    } else if (tif === 'IOC') {
      o.status = 'EXPIRED';
    } else {
      this.resting.push(o);
    }
    return this.outcome(true, true, trades, o);
  }

  cancel(orderId: number): MiniOutcome {
    const o = this.orders.get(orderId);
    if (!o || o.status !== 'LIVE') {
      return {
        accepted: false,
        consumeReceive: false,
        trades: [],
        orderId,
        status: 'REJECTED',
        tradedQty: 0,
        restingQty: 0,
        errorCode: 'UNKNOWN_ORDER',
      };
    }
    o.status = 'CANCELLED';
    this.resting = this.resting.filter((x) => x.id !== o.id);
    return this.outcome(true, false, [], o);
  }

  amend(
    orderId: number,
    newSeq: number,
    patch: { price?: number; qty?: number },
  ): MiniOutcome {
    const o = this.orders.get(orderId);
    if (!o || o.status !== 'LIVE') {
      return {
        accepted: false,
        consumeReceive: false,
        trades: [],
        orderId,
        status: 'REJECTED',
        tradedQty: 0,
        restingQty: 0,
        errorCode: 'UNKNOWN_ORDER',
      };
    }
    const price = patch.price ?? o.price;
    const qty = patch.qty ?? o.qty;
    if (qty <= o.filled) {
      return {
        accepted: false,
        consumeReceive: false,
        trades: [],
        orderId,
        status: 'REJECTED',
        tradedQty: 0,
        restingQty: 0,
        errorCode: 'INVALID_ARGUMENT',
      };
    }
    const requeued = price !== o.price || qty > o.qty;
    o.price = price;
    o.qty = qty;
    if (requeued) {
      this.resting = this.resting.filter((x) => x.id !== o.id);
      o.seq = newSeq;
      const trades = this.cross(o, newSeq);
      if (o.filled === o.qty) {
        o.status = 'FILLED';
      } else {
        this.resting.push(o);
      }
      return this.outcome(true, true, trades, o);
    }
    return this.outcome(true, false, [], o);
  }

  private cross(taker: MiniOrder, tradeSeq: number): MiniTrade[] {
    const trades: MiniTrade[] = [];
    let n = 0;
    while (taker.filled < taker.qty) {
      const candidates = this.resting
        .filter((m) => {
          if (m.side === taker.side || m.status !== 'LIVE') return false;
          return taker.side === 'BUY'
            ? m.price <= taker.price
            : m.price >= taker.price;
        })
        .sort((a, b) => {
          // 价格优先，再按接收序号（小者优先）
          if (a.price !== b.price) {
            return taker.side === 'BUY'
              ? a.price - b.price
              : b.price - a.price;
          }
          return a.seq - b.seq;
        });
      const maker = candidates[0];
      if (!maker) break;
      const qty = Math.min(taker.qty - taker.filled, maker.qty - maker.filled);
      n += 1;
      trades.push({
        tradeId: `T${tradeSeq}-${n}`,
        price: maker.price,
        qty,
        makerId: maker.id,
        takerId: taker.id,
      });
      taker.filled += qty;
      maker.filled += qty;
      if (maker.filled === maker.qty) {
        maker.status = 'FILLED';
        this.resting = this.resting.filter((x) => x.id !== maker.id);
      }
    }
    return trades;
  }

  private outcome(
    accepted: boolean,
    consumeReceive: boolean,
    trades: MiniTrade[],
    o: MiniOrder,
  ): MiniOutcome {
    return {
      accepted,
      consumeReceive,
      trades,
      orderId: o.id,
      status: o.status,
      tradedQty: trades
        .filter((t) => t.takerId === o.id)
        .reduce((s, t) => s + t.qty, 0),
      restingQty: o.status === 'LIVE' ? o.qty - o.filled : 0,
    };
  }

  private rejected(errorCode: string): MiniOutcome {
    return {
      accepted: false,
      consumeReceive: false,
      trades: [],
      orderId: 0,
      status: 'REJECTED',
      tradedQty: 0,
      restingQty: 0,
      errorCode,
    };
  }

  /** 当前每个价位的聚合剩余量（差分比对用） */
  levels(): {
    bids: Array<{ price: number; qty: number }>;
    asks: Array<{ price: number; qty: number }>;
  } {
    const agg = (side: 'BUY' | 'SELL') => {
      const map = new Map<number, number>();
      for (const o of this.resting) {
        if (o.side !== side) continue;
        map.set(o.price, (map.get(o.price) ?? 0) + (o.qty - o.filled));
      }
      return [...map.entries()]
        .map(([price, qty]) => ({ price, qty }))
        .sort((a, b) =>
          side === 'BUY' ? b.price - a.price : a.price - b.price,
        );
    };
    return { bids: agg('BUY'), asks: agg('SELL') };
  }

  order(id: number): MiniOrder | undefined {
    return this.orders.get(id);
  }
}
