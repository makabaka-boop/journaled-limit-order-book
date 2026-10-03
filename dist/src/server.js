// WebSocket front-end for the matching service.
//
// Wire protocol (JSON, one message per frame):
//   client -> server:
//     { "type": "subscribe", "lastSeq"?: number }
//     { "type": "submit", "key"?: string, "request": TradeRequest }
//   server -> client:
//     "hello" | "caught_up" | "snapshot" | "event" | "receipt" | "error"
//
// A connection holds at most one live subscription; resubscribing replaces
// it. Subscriptions resume by event seq: buffered gaps replay directly;
// gaps beyond retained history receive a snapshot FIRST, then events.
import { createServer } from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';
import { Publisher } from './publisher.js';
import { MatchingService } from './service.js';
export function startWsServer(options) {
    const serviceOpts = {};
    if (options.snapshotEveryEvents !== undefined)
        serviceOpts.snapshotEveryEvents = options.snapshotEveryEvents;
    if (options.retainSnapshots !== undefined)
        serviceOpts.retainSnapshots = options.retainSnapshots;
    if (options.publisherBufferSize !== undefined)
        serviceOpts.publisherBufferSize = options.publisherBufferSize;
    const service = MatchingService.open(options.dbPath, serviceOpts);
    const http = createServer();
    const wss = new WebSocketServer({ server: http });
    wss.on('connection', (ws, _req) => {
        let subscribed = false;
        const send = (msg) => {
            if (ws.readyState === WebSocket.OPEN)
                ws.send(JSON.stringify(msg));
        };
        ws.on('message', async (raw) => {
            let msg;
            try {
                msg = JSON.parse(raw.toString());
            }
            catch {
                const err = { type: 'error', code: 'BAD_MESSAGE', message: 'invalid JSON' };
                send(err);
                return;
            }
            if (msg === null || typeof msg !== 'object' || typeof msg.type !== 'string') {
                send({ type: 'error', code: 'BAD_MESSAGE', message: 'missing message type' });
                return;
            }
            try {
                if (msg.type === 'subscribe') {
                    const lastSeq = typeof msg.lastSeq === 'number' && Number.isSafeInteger(msg.lastSeq) && msg.lastSeq >= 0
                        ? msg.lastSeq
                        : undefined;
                    // Replace any previous subscription on the same socket.
                    if (subscribed)
                        service.publisher.unsubscribe(send);
                    subscribed = false;
                    await service.publisher.subscribe(send, lastSeq, service.lastCommitSeq(), service.ledger);
                    subscribed = true;
                    return;
                }
                if (msg.type === 'submit') {
                    const envelope = parseSubmit(msg);
                    if (envelope instanceof Error) {
                        send({ type: 'error', code: 'BAD_MESSAGE', message: envelope.message });
                        return;
                    }
                    const result = await service.submit(envelope);
                    if (!result.ok) {
                        send({
                            type: 'error',
                            code: result.errorCode ?? 'INVALID_REQUEST',
                            message: result.errorMessage ?? 'rejected',
                            ...(envelope.key !== undefined ? { key: envelope.key } : {}),
                        });
                        return;
                    }
                    const receipt = {
                        type: 'receipt',
                        ...(envelope.key !== undefined ? { key: envelope.key } : {}),
                        receipt: result.receipt,
                    };
                    send(receipt);
                    return;
                }
                send({ type: 'error', code: 'PROTOCOL', message: `unknown type ${msg.type}` });
            }
            catch (err) {
                send({ type: 'error', code: 'PROTOCOL', message: err.message });
            }
        });
        ws.on('close', () => {
            service.publisher.unsubscribe(send);
        });
        ws.on('error', () => {
            service.publisher.unsubscribe(send);
        });
    });
    return new Promise((resolve, reject) => {
        http.on('error', reject);
        http.listen(options.port ?? 0, '127.0.0.1', () => {
            const port = http.address().port;
            resolve({
                port,
                service,
                close: async () => {
                    // Terminate any lingering client sockets so shutdown cannot block
                    // on half-open connections; then close the servers and the DB.
                    for (const client of wss.clients) {
                        client.terminate();
                    }
                    await new Promise((res) => wss.close(() => res()));
                    await new Promise((res) => http.close(() => res()));
                    service.close();
                },
            });
        });
    });
}
function parseSubmit(msg) {
    if (msg === null || typeof msg !== 'object')
        return new Error('submit must be an object');
    const request = msg.request;
    if (request === null || typeof request !== 'object') {
        return new Error('submit.request required');
    }
    const key = msg.key;
    if (key !== undefined && typeof key !== 'string')
        return new Error('key must be a string');
    return {
        ...(typeof key === 'string' ? { key } : {}),
        request: request,
    };
}
// Re-export for programmatic embedding.
export { MatchingService, Publisher };
//# sourceMappingURL=server.js.map