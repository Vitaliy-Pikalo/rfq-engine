# rfq-engine

A request-for-quote (RFQ) engine in Go: a client asks for a price, the engine fans the request out to a panel of dealers, collects firm quotes with a time-to-live, ranks them, and runs the accept → last look → fill flow. It speaks a FIX 4.4-style tag=value protocol over TCP.

I built this to understand how institutional RFQ venues work under the hood: concurrency across dealers, quote expiry, the last-look problem, and what "best price" actually means when dealers can reject you.

## What it does

```
client ──QuoteRequest(R)──▶ engine ──fan out──▶ dealer 1..N   (each: spread, latency, decline rate)
       ◀──Quote(S) x N──────        ◀──quotes within collect window
       ──NewOrderSingle(D)─▶ engine ──last look──▶ winning dealer
       ◀──ExecutionReport(8)       ◀──accept / reject
```

RFQ lifecycle (enforced by an explicit transition table, illegal moves return errors):

```
PENDING ──quotes──▶ QUOTED ──accept──▶ ACCEPTED ──last look ok──▶ FILLED
   │                  │                    └────last look reject──▶ REJECTED
   └──none──▶ NO_QUOTES └──every quote's TTL passed──▶ EXPIRED
```

- **Fan-out with a deadline.** Every dealer is asked in parallel. Quotes that arrive after the collect window are dropped; the engine never waits on the slowest dealer.
- **Firm quotes with TTL.** Accepting a stale quote fails with `ErrQuoteExpired`. One stale quote doesn't kill the RFQ if others are still live.
- **Last look with a timeout.** The dealer gets a bounded window to reject. It runs outside the engine lock so one slow dealer can't stall other RFQs. Answering after the timeout counts as a reject.
- **Exactly-once accept.** 50 goroutines racing to accept the same RFQ: one wins, the dealer sees one last look (tested).
- **Bad dealer input is filtered.** Crossed, zero, NaN or infinite prices are dropped and counted.
- **Reject-adjusted ranking (optional).** See below; this was the most interesting result.

## The interesting part: best price ≠ best execution

The demo dealer panel includes `tightmm`, which shows the tightest spread but has a 0.5 bp last-look tolerance. Ranking purely by price sends it most of the flow, and it rejects about a third of accepts.

With `-penalty N`, the engine ranks quotes by a reject-adjusted price: each dealer's quote is worsened by `observed reject rate × N bps` (after a minimum sample size). The client still trades at the quoted price; only the ranking changes.

20,000 RFQs, 5 simulated dealers, 16 connections × 4 in flight, 80% of quotes accepted:

| ranking | fill rate (of accepts) | last-look rejects | avg cost vs quote mid |
|---|---|---|---|
| raw price (`-penalty 0`) | 69.4% | 30.6% | 1.75 bps |
| reject-adjusted (`-penalty 5`) | 94.0% | 6.0% | 3.05 bps |
| reject-adjusted (`-penalty 10`) | 97.5% | 2.5% | 3.17 bps |

The trade-off: about 1.3 bps more cost buys about 25 points of fill certainty. Whether that's worth it depends on what a reject costs you (re-quoting in a market that just moved against you is usually worse than 1.3 bps, which is exactly why the dealer rejected). Real venues track dealer reject rates and hold-times for this reason.

## Performance

Measured on a 2-vCPU Intel Xeon @ 2.1 GHz Linux VM, loopback TCP, `go run ./cmd/rfqload`.

**Engine + protocol overhead** (instant dealers, so this is just the engine, codec and TCP):

| load | throughput | quote round trip p50 / p99 | accept round trip p50 / p99 |
|---|---|---|---|
| 1 connection, 1 in flight | 9,238 RFQ/s | 65 µs / 247 µs | 19 µs / 111 µs |
| 16 connections × 4 in flight | 16,034 RFQ/s | 2.0 ms / 11.7 ms | 1.5 ms / 9.4 ms |

**With the simulated dealer panel** (50 ms collect window): time-to-quote p50 is 45.6 ms and p99 51.3 ms, set by the `slow` dealer, which misses the window on about 37% of RFQs. That's the window doing its job: the RFQ completes on time with the other four quotes.

Numbers will differ on your machine; run the load test to reproduce.

## Run it

Requires Go 1.22+.

```bash
go test -race ./...                      # full test suite with the race detector
go run ./cmd/rfqload -inproc             # load test against an in-process server
go run ./cmd/rfqload -inproc -penalty 5  # same, with reject-adjusted ranking
go run ./cmd/rfqload -inproc -instant-dealers -clients 1 -inflight 1   # raw overhead

go run ./cmd/rfqd                        # standalone server on 127.0.0.1:9878
go run ./cmd/rfqload -addr 127.0.0.1:9878
```

## Protocol

FIX 4.4-style framing: `8=FIX.4.4|9=<BodyLength>|35=<MsgType>|...|10=<CheckSum>|` (shown with `|` for SOH). Body length and checksum follow the FIX definitions; the decoder caps body size at 64 KiB and is fuzz-tested.

| MsgType | direction | meaning | key tags |
|---|---|---|---|
| `R` QuoteRequest | client → server | ask for quotes | 131 QuoteReqID, 55 Symbol, 54 Side (1 buy / 2 sell), 38 OrderQty |
| `S` Quote | server → client | one per dealer, best first | 117 QuoteID, 448 dealer, 132 BidPx, 133 OfferPx, 62 ValidUntilTime, 9001 QuoteCount, 9002 QuoteRank |
| `AG` QuoteRequestReject | server → client | invalid request or no quotes | 131, 58 Text |
| `D` NewOrderSingle | client → server | accept a quote | 131, 117, 11 ClOrdID |
| `8` ExecutionReport | server → client | fill or reject | 39 OrdStatus (2 filled / 8 rejected), 31 LastPx, 32 LastQty, 58 Text |
| `3` Reject | server → client | malformed or unsupported message | 58 Text |

Tags 9001/9002 are in FIX's user-defined range. This is not a full FIX engine: no logon, sequence numbers or resend.

## Layout

```
internal/rfq      lifecycle state machine, fan-out, ranking, last look   (engine.go, types.go)
internal/dealer   simulated dealers: spread widening with size, latency, declines, price-tolerance last look
internal/fixmsg   tag=value codec with BodyLength/CheckSum framing (+ fuzz test)
internal/server   TCP server and a concurrent client that demuxes replies by QuoteReqID
internal/pricing  random-walk reference mid prices
internal/metrics  ring-buffer latency histograms and counters
cmd/rfqd          standalone server
cmd/rfqload       load generator that prints the tables above
```

## Testing

- Table-driven tests for request validation and every legal/illegal state transition
- Fake clock for TTL and expiry edge cases (accepting exactly at `ExpiresAt` counts as expired)
- Concurrency tests (racing accepts, 200 concurrent RFQs, 100 RFQs multiplexed on one TCP connection), all under `-race`
- End-to-end TCP tests including garbage input, unsupported messages and shutdown with live connections
- `FuzzReader` for the decoder: `go test -fuzz=FuzzReader ./internal/fixmsg`

## Not built (yet)

- Real FIX session layer (logon, sequence numbers, gap fill)
- Streaming quotes that update until expiry; partial fills
- Persistence and an audit log of every state transition
- Per-client credit limits checked before fan-out
