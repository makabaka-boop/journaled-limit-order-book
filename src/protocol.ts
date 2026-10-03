import type { OrderCommand, Side, Tif } from './types.js';

export interface ParsedCommand {
  command?: OrderCommand;
  error?: string;
}

function asSide(v: unknown): Side | null {
  return v === 'BUY' || v === 'SELL' ? v : null;
}

function asTif(v: unknown): Tif | null {
  return v === 'GTC' || v === 'IOC' ? v : null;
}

function asPositiveInt(v: unknown): number | null {
  if (typeof v === 'number' && Number.isInteger(v) && v > 0) return v;
  if (typeof v === 'string' && /^\d+$/.test(v)) {
    const n = Number(v);
    return n > 0 ? n : null;
  }
  return null;
}

/** 解析 /orders 挂单请求体 */
export function parsePlaceBody(body: unknown): ParsedCommand {
  if (!body || typeof body !== 'object') {
    return { error: '请求体必须是 JSON 对象' };
  }
  const b = body as Record<string, unknown>;
  const side = asSide(b.side);
  const tif = asTif(b.tif ?? 'GTC');
  const price = asPositiveInt(b.price);
  const qty = asPositiveInt(b.qty);
  if (!side) return { error: 'side 必须为 BUY 或 SELL' };
  if (!tif) return { error: 'tif 必须为 GTC 或 IOC' };
  if (price === null) return { error: 'price 必须为正整数' };
  if (qty === null) return { error: 'qty 必须为正整数' };
  return { command: { kind: 'place', side, price, qty, tif } };
}

/** 解析 /orders/:id/amend 改单请求体 */
export function parseAmendBody(orderId: unknown, body: unknown): ParsedCommand {
  const id = asPositiveInt(orderId);
  if (id === null) return { error: '订单号必须为正整数' };
  const b = (body ?? {}) as Record<string, unknown>;
  const cmd: OrderCommand = { kind: 'amend', orderId: id };
  let touched = false;
  if (b.price !== undefined) {
    const price = asPositiveInt(b.price);
    if (price === null) return { error: 'price 必须为正整数' };
    cmd.price = price;
    touched = true;
  }
  if (b.qty !== undefined) {
    const qty = asPositiveInt(b.qty);
    if (qty === null) return { error: 'qty 必须为正整数' };
    cmd.qty = qty;
    touched = true;
  }
  if (!touched) return { error: '改单至少需要提供 price 或 qty' };
  return { command: cmd };
}

export function parseCancel(orderId: unknown): ParsedCommand {
  const id = asPositiveInt(orderId);
  if (id === null) return { error: '订单号必须为正整数' };
  return { command: { kind: 'cancel', orderId: id } };
}
