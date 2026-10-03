import { createHash } from 'node:crypto';
import type { Socket } from 'node:net';
import type { SubscriberTransport } from './publisher.js';

/**
 * 最小 RFC6455 WebSocket 服务端（文本帧）。
 * 仅实现发布器所需能力：握手、客户端分片合并、文本消息回调、
 * 服务端发送文本帧、ping/pong 与关闭。
 */
export class WebSocketConnection implements SubscriberTransport {
  private readonly socket: Socket;
  private fragments: Buffer[] | null = null;
  private closed = false;
  onMessage: (text: string) => void = () => {};
  onClose: () => void = () => {};

  /**
   * @param head Node `upgrade` 事件的第三个参数：与升级请求在同一个 TCP
   *   数据块里到达的残留字节（客户端常把 subscribe 帧与握手合并发送，
   *   不消费它会导致首帧丢失）。
   */
  constructor(socket: Socket, key: string, head?: Buffer) {
    this.socket = socket;
    const accept = createHash('sha1')
      .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
      .digest('base64');
    socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
    );
    socket.on('data', (chunk: Buffer) => this.feed(chunk));
    socket.on('close', () => {
      this.closed = true;
      this.onClose();
    });
    socket.on('error', () => {
      this.closed = true;
    });
    // 延迟到下一个 tick：让调用方先完成 onMessage/onClose 赋值。
    if (head && head.length > 0) {
      process.nextTick(() => this.feed(head));
    }
  }

  static readonly WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

  private feed(chunk: Buffer): void {
    // 服务端握手完成后收到的都是 WS 帧。
    this.parseFrames(chunk);
  }

  private parseFrames(initial: Buffer): void {
    let data: Buffer = initial.length > 0
      ? Buffer.concat([...this.pendingFrames, initial])
      : Buffer.concat(this.pendingFrames);
    this.pendingFrames.length = 0;

    while (data.length >= 2) {
      const b0 = data[0]!;
      const b1 = data[1]!;
      const fin = (b0 & 0x80) === 0x80;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) === 0x80;
      let len = b1 & 0x7f;
      let offset = 2;

      if (len === 126) {
        if (data.length < offset + 2) {
          this.pendingFrames.push(data);
          return;
        }
        len = data.readUInt16BE(offset);
        offset += 2;
      } else if (len === 127) {
        if (data.length < offset + 8) {
          this.pendingFrames.push(data);
          return;
        }
        len = Number(data.readBigUInt64BE(offset));
        offset += 8;
      }

      let mask: Buffer | null = null;
      if (masked) {
        if (data.length < offset + 4) {
          this.pendingFrames.push(data);
          return;
        }
        mask = data.subarray(offset, offset + 4);
        offset += 4;
      }
      if (data.length < offset + len) {
        this.pendingFrames.push(data);
        return;
      }

      const payload = data.subarray(offset, offset + len);
      if (masked && mask) {
        const maskBuf: Buffer = mask;
        const unmasked = Buffer.allocUnsafe(len);
        for (let i = 0; i < len; i++) {
          unmasked[i] = payload[i]! ^ maskBuf[i % 4]!;
        }
        this.dispatch(opcode, fin, unmasked);
      } else {
        this.dispatch(opcode, fin, Buffer.from(payload));
      }
      data = data.subarray(offset + len);
    }
    if (data.length > 0) this.pendingFrames.push(data);
  }

  private pendingFrames: Buffer[] = [];

  private dispatch(opcode: number, fin: boolean, payload: Buffer): void {
    if (opcode === 0x8) {
      // close
      this.close();
      return;
    }
    if (opcode === 0x9) {
      // ping -> pong
      this.sendFrame(0xa, payload);
      return;
    }
    if (opcode === 0xa) return; // pong

    if (opcode === 0x1) this.fragments = [];
    if (opcode === 0x0 || opcode === 0x1) {
      this.fragments?.push(payload);
      if (fin && this.fragments) {
        const text = Buffer.concat(this.fragments).toString('utf8');
        this.fragments = null;
        if (!this.closed) this.onMessage(text);
      }
    }
  }

  private sendFrame(opcode: number, payload: Buffer): void {
    if (this.closed) return;
    const len = payload.length;
    let header: Buffer;
    if (len < 126) {
      header = Buffer.alloc(2);
      header[1] = len;
    } else if (len <= 0xffff) {
      header = Buffer.alloc(4);
      header[1] = 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.alloc(10);
      header[1] = 127;
      header.writeBigUInt64BE(BigInt(len), 2);
    }
    header[0] = 0x80 | opcode;
    this.socket.write(Buffer.concat([header, payload]));
  }

  send(text: string): void {
    this.sendFrame(0x1, Buffer.from(text, 'utf8'));
  }

  isOpen(): boolean {
    return !this.closed && !this.socket.destroyed;
  }

  close(): void {
    if (this.closed) return;
    try {
      this.sendFrame(0x8, Buffer.alloc(0));
    } catch {
      // ignore
    }
    this.closed = true;
    this.socket.end();
  }
}
