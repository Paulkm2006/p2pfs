# UDP shard transport

`Transport` uses Pion WebRTC v4 reliable, ordered DataChannels: SCTP congestion
control/retransmission over authenticated DTLS over UDP ICE. `SetNetworkTypes`
allows UDP4/UDP6 only. ICE URLs are limited to `stun:` and UDP `turn:`; TCP TURN,
TLS TURN and ICE TCP are rejected. There is no HTTP/TCP shard fallback.

## Connection and identity

- `Fetch` gathers host/STUN candidates with no TURN configured and allows eight
  seconds for a direct connection by default. On setup failure it starts a fresh,
  bounded relay-only connection if TURN was supplied. `WithForceRelay` skips the
  direct attempt. Both peers enforce relay policy via the authenticated `Relay`
  field. A shard transfer is never restarted after writing partial output.
- The signaling coordinator must authenticate `Signal.From`, deliver only to the
  authenticated `To`, and protect SDP with TLS. DTLS verifies the SDP fingerprint;
  signaling authenticity is consequently part of the transport trust boundary.
- Offers/answers use a random 128-bit transaction ID. Answers must match the
  outstanding transaction, sender and relay mode. Active duplicates and a bounded
  ten-minute recent-transaction cache reject replays.
- `Stats.Path` comes from the actual selected ICE candidate pair, not from the
  requested mode. The selected pair is also checked to be UDP.

## Wire protocol and resource limits

Each connection carries exactly one `p2pfs-shard-v1` DataChannel and one request.
The channel is explicitly reliable and ordered. The requester sends a bounded
JSON `{file_id,index}` message. The source sends a JSON header with byte size,
then binary messages of at most 16 KiB, then an ordered JSON end marker with final
byte count or a generic error. Missing/trailing bytes, invalid controls and
oversized blocks fail the transfer. File IDs are lowercase SHA-256 hex; indices
are 0–5. A shard cannot exceed the fixed 1 GiB / 4 data-shard model's maximum.

The caller must verify the received shard hash and manifest length before making
it available. `Fetch` can leave partial bytes in its writer on failure, so callers
should write into a disposable temporary file. Writers must return promptly;
the transport cannot interrupt an arbitrary blocking `io.Writer.Write`.

The sender's queued DataChannel bytes, receiver's message queue, and SCTP receive
window are each bounded near 256 KiB. Reader buffers are at most 16 KiB; no whole
shard is loaded into memory. Aggregate upload payload pacing is optional through
`WithUploadBytesPerSecond`. The default cap is 32 simultaneous sessions and eight
sessions per remote peer. Sessions have a ten-minute maximum lifetime.
Cancellation closes the PeerConnection and source reader. The receiver closes
only after consuming the end frame, allowing queued SCTP messages to drain.

`OpenShard` must enforce current shared holdings and return a reader whose `Close`
interrupts `Read`. `StopSharing(fileID)` cancels current incoming transfers; the
owner must revoke the holding first to reject future opens. An already executing
`OpenShard` callback cannot be interrupted by its function signature, but its
returned reader is closed without transmitting bytes if sharing was canceled.

## Verification

Run `go test -race ./internal/transport/...`. Tests exercise actual loopback UDP,
STUN, DTLS, SCTP, and Pion TURN traffic, including automatic fallback, cancellation,
quota checks, replay/sender binding, rate limits, empty files, inconsistent source
lengths and unsharing while a source is opening.

The explicit `testnet.Loopback()` fixture replaces only host interface discovery
for constrained CI runners that prohibit netlink. Production does not use it or
automatically switch to it. Loopback tests do not establish cross-NAT reachability,
real-world packet-loss throughput, or connectivity on a UDP-blocked network.

Pion references: [API](https://pkg.go.dev/github.com/pion/webrtc/v4),
[flow control example](https://github.com/pion/webrtc/tree/main/examples/data-channels-flow-control).
