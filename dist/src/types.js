// Domain types for the single-market matching service.
//
// Sequencing model
// ----------------
// recvSeq  : server receive sequence. Assigned monotonically in receive
//            order to each processed request. Ties at the same price are
//            broken by the lower recvSeq ("server receive order wins").
// commitSeq: recvSeq of the latest request whose effects were durably
//            committed. The engine, SQLite ledger and WebSocket publisher
//            all advance on this number: nothing is published until the
//            commit for that seq is durable.
// eventSeq : 1-based global ordinal of a ledger event. One commit may
//            contain several events. Subscribers resume by this number;
//            it is consumed from SQLite AUTOINCREMENT and never reused.
export {};
//# sourceMappingURL=types.js.map