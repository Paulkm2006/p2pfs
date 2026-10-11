# Metadata coordination and bounded UDP relay

`New(Config)` opens SQLite and optionally starts TURN; `Handler()` exposes Echo v5
as `http.Handler`; `Close()` is idempotent and stops relay sockets and expiry work.
An embedding application must provide HTTPS and HTTP timeouts. `cmd/server` does
so and rejects plaintext except an explicit loopback development listener.

## Identity and persistence

This is one trusted collaboration space, **not an Internet-ready admission or
account system**. Anyone who can reach registration can join the space and see
its metadata. Restrict access with a private network/firewall or an authenticated
reverse proxy before deployment. HTTPS authenticates the coordinator; UUIDs are
identifiers, not credentials.

Registration generates a UUID v4, independent 256-bit device secret, and a fresh
256-bit bearer token. Only the SHA-256 hash of the uniformly random device secret
is stored. Possession of the UUID alone cannot reconnect. Re-registration requires
the secret, replaces the old session and gives fresh TURN credentials. Sessions
expire absolutely after 24 hours, and are memory-only. Heartbeat is every 5 seconds;
sources stop being advertised after 15 seconds without heartbeat. Expiry scanning
runs every second. Clients must re-register/reconfirm local holdings after restart
or session replacement. Persisted holdings carry the former session hash and
cannot silently become online again.

SQLite stores identities, immutable validated manifests, and shard indices, never
file payloads. The database is created with owner-only access, foreign keys, WAL,
and a busy timeout. Keep its containing directory and backups private as well.
Only one coordinator instance should open the same database for service.

## API

Only `/healthz` and `POST /v1/register` are unauthenticated. Every other endpoint
requires `Authorization: Bearer <session token>`:

- `POST /v1/heartbeat` refreshes last-seen, but not absolute session expiry.
- `POST /v1/manifests` validates the immutable content-addressed manifest.
- `GET /v1/files` and `GET /v1/files/:id` return manifests and currently online,
  session-reconfirmed holdings. Online does not prove reachability or valid bytes.
- `PUT /v1/holdings` replaces only the authenticated peer's indices for a file;
  an empty list revokes them.
- `POST /v1/signals` queues `offer`/`answer` SDP; the sender is overwritten by the
  authenticated identity. `GET /v1/signals` drains the caller's inbox. Expired
  messages and messages from replaced/offline sessions are discarded. Signals
  are ephemeral; callers must retry a timed-out handshake with a new request ID.

## TURN restrictions

The optional embedded Pion TURN server has **only a UDP listener and UDP relay
allocations**. TLS/HTTPS is for metadata only; file bytes never fall back to TCP.
The relay is not a file holder and does not persist relayed bytes. Payload
confidentiality/integrity is provided end-to-end by the client's WebRTC DTLS stack.

Both endpoints of a relayed connection must use relay candidates on this same
server. Permissions only admit the configured relay IP, and every packet is
further checked against an exact live allocation IP+port with valid sessions on
both ends. Arbitrary public hosts, private hosts, metadata endpoints and unrelated
local ports cannot be targets. Traffic between two allocations is routed over
local UDP sockets, so public-IP NAT hairpin support is unnecessary. Session
revocation/expiry blocks existing channel data immediately; a one-second reaper
also closes invalid or idle sockets. Loopback relay configuration requires an
explicit development flag and a loopback listener. `Network` allows test-only
socket injection in runners that cannot enumerate network interfaces.

Default hard bounds:

- 64 sessions, 1,000 persisted devices, 10,000 manifests
- 64 KiB JSON body; 48 KiB SDP; 64 queued signals and 512 KiB retained signaling per recipient; 60-second signal TTL
- 500 API requests/second globally, 100/session/second, 120 registrations/minute
- 128 relay allocations, 8/session; UDP packet limit 1,500 bytes
- 64 MiB/s aggregate relay traffic and 8 MiB/s per session (send and receive combined)
- 90-second relay idle timeout; no unbounded in-memory payload queues

Bandwidth uses bounded one-second buckets and drops excess UDP packets; the
client's SCTP congestion handling responds to loss. These are experimental defaults,
not demonstrated throughput guarantees or comprehensive DDoS protection. Keep OS
UDP buffers, firewall rate controls, reverse-proxy limits, and monitoring bounded
for your deployment. New identities/manifests are refused at capacity; there is no
administrative pruning API yet. Defaults are configurable in `Config`/`TURNConfig`.

## Verification and library references

Tests exercise registration spoofing/replacement, hash-only secret persistence,
manifest validation, holdings isolation/revocation, heartbeat/absolute expiry,
restart/reconfirmation, bounded signaling, plaintext listener refusal, actual
Pion TURN payload forwarding, arbitrary target rejection, relay expiry and quotas.
The root integration suite adds client/WebRTC direct and relay transfers.

- [Echo v5 API](https://pkg.go.dev/github.com/labstack/echo/v5)
- [Pion TURN v4 API](https://pkg.go.dev/github.com/pion/turn/v4)
- [Pion relay generator and permission interfaces](https://github.com/pion/turn/blob/v4.1.4/server_config.go)
- [SQLite WAL](https://www.sqlite.org/wal.html)
