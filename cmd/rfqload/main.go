// Command rfqload drives an rfqd server with concurrent clients and reports
// client-observed latency and outcome rates.
//
//	go run ./cmd/rfqload -inproc -clients 16 -rfqs 20000
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Vitaliy-Pikalo/rfq-engine/internal/dealer"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/metrics"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/pricing"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/rfq"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9878", "server address (ignored with -inproc)")
	inproc := flag.Bool("inproc", false, "start a server in this process on a random port")
	clients := flag.Int("clients", 16, "concurrent client connections")
	inflight := flag.Int("inflight", 4, "concurrent RFQs per connection")
	total := flag.Int("rfqs", 10_000, "total RFQs to send")
	acceptPct := flag.Float64("accept", 0.8, "fraction of quoted RFQs the client accepts")
	collect := flag.Duration("collect", 50*time.Millisecond, "dealer collect window (-inproc only)")
	instant := flag.Bool("instant-dealers", false, "zero-latency dealers (-inproc only): measures engine+protocol overhead")
	penalty := flag.Float64("penalty", 0, "last-look penalty in bps for reject-adjusted ranking (-inproc only; 0 = rank by raw price)")
	flag.Parse()

	target := *addr
	var eng *rfq.Engine
	if *inproc {
		var stop func()
		var err error
		target, eng, stop, err = startInproc(*collect, *instant, *penalty)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer stop()
	}

	res := drive(target, *clients, *inflight, *total, *acceptPct)
	res.print(os.Stdout)
	if eng != nil {
		fmt.Println("\nserver-side metrics:")
		fmt.Print(eng.Stats().Report())
	}
}

type results struct {
	elapsed                         time.Duration
	quoted, noQuote, errs           atomic.Int64
	filled, llRejected, expiredOrNo atomic.Int64
	quoteRTT, fillRTT               *metrics.Histogram

	mu         sync.Mutex
	costBpsSum float64 // sum over fills of |price - quote mid| in bps
}

func drive(addr string, clients, inflight, total int, acceptPct float64) *results {
	res := &results{quoteRTT: metrics.NewHistogram(1 << 20), fillRTT: metrics.NewHistogram(1 << 20)}
	symbols := []string{"BTC-USD", "ETH-USD", "SOL-USD"}
	var next atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()

	for ci := 0; ci < clients; ci++ {
		c, err := server.Dial(context.Background(), addr)
		if err != nil {
			fmt.Fprintln(os.Stderr, "dial:", err)
			os.Exit(1)
		}
		defer c.Close()
		for w := 0; w < inflight; w++ {
			wg.Add(1)
			go func(seed int64) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(seed))
				for {
					n := next.Add(1)
					if n > int64(total) {
						return
					}
					side := rfq.Buy
					if rng.Intn(2) == 1 {
						side = rfq.Sell
					}
					req := rfq.Request{
						ID:     fmt.Sprintf("load-%d", n),
						Symbol: symbols[rng.Intn(len(symbols))],
						Side:   side,
						Qty:    float64(1 + rng.Intn(10)),
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					t0 := time.Now()
					qs, err := c.RequestQuotes(ctx, req)
					res.quoteRTT.Observe(time.Since(t0))
					switch {
					case errors.Is(err, server.ErrRejected):
						res.noQuote.Add(1)
						cancel()
						continue
					case err != nil:
						res.errs.Add(1)
						cancel()
						continue
					}
					res.quoted.Add(1)
					if rng.Float64() >= acceptPct {
						cancel()
						continue
					}
					t1 := time.Now()
					f, err := c.Accept(ctx, req.ID, qs[0].ID)
					res.fillRTT.Observe(time.Since(t1))
					cancel()
					switch {
					case err != nil:
						res.expiredOrNo.Add(1)
					case f.Filled:
						res.filled.Add(1)
						mid := (qs[0].Bid + qs[0].Offer) / 2
						cost := (f.Price - mid) / mid * 1e4
						if cost < 0 {
							cost = -cost
						}
						res.mu.Lock()
						res.costBpsSum += cost
						res.mu.Unlock()
					default:
						res.llRejected.Add(1)
					}
				}
			}(int64(ci*1000 + w))
		}
	}
	wg.Wait()
	res.elapsed = time.Since(start)
	return res
}

func (r *results) print(w *os.File) {
	q, f := r.quoteRTT.Snapshot(), r.fillRTT.Snapshot()
	accepted := r.filled.Load() + r.llRejected.Load()
	fmt.Fprintf(w, "rfqs:        %d in %v (%.0f rfq/s)\n", q.Count, r.elapsed.Round(time.Millisecond), float64(q.Count)/r.elapsed.Seconds())
	fmt.Fprintf(w, "quoted:      %d   no-quote: %d   errors: %d\n", r.quoted.Load(), r.noQuote.Load(), r.errs.Load())
	fmt.Fprintf(w, "accepts:     %d   filled: %d   last-look rejected: %d (%.1f%%)   other: %d\n",
		accepted, r.filled.Load(), r.llRejected.Load(), pct(r.llRejected.Load(), accepted), r.expiredOrNo.Load())
	if r.filled.Load() > 0 {
		fmt.Fprintf(w, "fill rate:   %.1f%% of accepts   avg cost vs quote mid: %.2f bps\n",
			pct(r.filled.Load(), accepted), r.costBpsSum/float64(r.filled.Load()))
	}
	fmt.Fprintf(w, "quote RTT:   p50=%v p90=%v p99=%v max=%v\n", q.P50, q.P90, q.P99, q.Max)
	fmt.Fprintf(w, "accept RTT:  p50=%v p90=%v p99=%v max=%v\n", f.P50, f.P90, f.P99, f.Max)
}

func pct(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return 100 * float64(a) / float64(b)
}

type instantDealer struct {
	name string
	feed *pricing.Feed
}

func (d instantDealer) Name() string { return d.name }
func (d instantDealer) Quote(_ context.Context, r rfq.Request) (float64, float64, error) {
	m, err := d.feed.Mid(r.Symbol)
	if err != nil {
		return 0, 0, rfq.ErrNoQuote
	}
	return m * 0.9998, m * 1.0002, nil
}
func (d instantDealer) LastLook(context.Context, rfq.Quote, rfq.Side) (bool, string) { return true, "" }

func startInproc(collect time.Duration, instant bool, penalty float64) (string, *rfq.Engine, func(), error) {
	feed := pricing.NewFeed(map[string]float64{"BTC-USD": 100_000, "ETH-USD": 4_000, "SOL-USD": 200}, 0.5, 42)
	var ds []rfq.Dealer
	if instant {
		for i := 0; i < 5; i++ {
			ds = append(ds, instantDealer{name: fmt.Sprintf("lp%d", i), feed: feed})
		}
	} else {
		for _, c := range dealer.DefaultRoster(42) {
			d, err := dealer.New(c, feed)
			if err != nil {
				return "", nil, nil, err
			}
			ds = append(ds, d)
		}
	}
	eng, err := rfq.NewEngine(rfq.Config{CollectWindow: collect, QuoteTTL: 2 * time.Second, LastLookPenaltyBps: penalty}, nil, ds...)
	if err != nil {
		return "", nil, nil, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, nil, err
	}
	srv := &server.Server{Engine: eng}
	go srv.Serve(ln)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		t := time.NewTicker(10 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				feed.Tick()
			}
		}
	}()
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				eng.Evict(time.Now().Add(-5 * time.Second))
			}
		}
	}()
	return ln.Addr().String(), eng, func() { cancel(); srv.Close() }, nil
}
