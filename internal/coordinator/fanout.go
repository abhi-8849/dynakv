package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/abhi-8849/dynakv/internal/ring"
)

type result[T any] struct {
	node ring.Node
	val  T
	err  error
}

// fanout runs call against every node in parallel and returns the first need successes. The calls use a context detached from the
// request, so slower replicas still finish their write after a quorum is reached; the timeout bounds them.
func fanout[T any](ctx context.Context, timeout time.Duration, nodes []ring.Node, need int,
	call func(context.Context, ring.Node) (T, error)) ([]result[T], error) {
	callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	// results is buffered for every call, so a call goroutine never blocks on send even after fanout has returned. Each goroutine
	// sends exactly once and ends within the timeout. fanout (through collect) receives; nothing closes the channel.
	results := make(chan result[T], len(nodes))
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := call(callCtx, n)
			results <- result[T]{node: n, val: v, err: err}
		}()
	}
	go func() { wg.Wait(); cancel() }() // releases the timeout once every call has finished
	return collect(ctx, callCtx, results, len(nodes), need)
}

// collect waits for need successes and gives up as soon as too many calls failed or the timeout passed.
func collect[T any](ctx, callCtx context.Context, results <-chan result[T], total, need int) ([]result[T], error) {
	var ok []result[T]
	var lastErr error
	failed := 0
	for len(ok) < need {
		if failed > total-need {
			return nil, fmt.Errorf("%w: %d of %d answered: %w", ErrUnavailable, len(ok), need, lastErr)
		}
		select {
		case r := <-results: // a replica answered or failed
			if r.err != nil {
				failed++
				lastErr = r.err
				continue
			}
			ok = append(ok, r)
		case <-ctx.Done(): // the caller gave up
			return nil, ctx.Err()
		case <-callCtx.Done(): // the replica timeout passed
			return nil, fmt.Errorf("%w: %d of %d answered before the timeout", ErrUnavailable, len(ok), need)
		}
	}
	return ok, nil
}
