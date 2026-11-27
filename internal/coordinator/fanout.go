package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/abhi-8849/dynakv/internal/ring"
)

// target is a node a request is sent to. hintFor is set when the node is a stand-in holding the write for that down owner.
type target struct {
	ring.Node
	hintFor string
}

type result[T any] struct {
	target target
	val    T
	err    error
}

// fanout runs call against every target in parallel and returns the first need successes. The calls use a context detached from the
// request, so slower replicas still finish their write after a quorum is reached; the timeout bounds them.
func fanout[T any](ctx context.Context, timeout time.Duration, targets []target, need int,
	call func(context.Context, target) (T, error)) ([]result[T], error) {
	callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	// results is buffered for every call, so a call goroutine never blocks on send even after fanout has returned. Each goroutine
	// sends exactly once and ends within the timeout. fanout (through collect) receives; nothing closes the channel.
	results := make(chan result[T], len(targets))
	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := call(callCtx, t)
			results <- result[T]{target: t, val: v, err: err}
		}()
	}
	go func() { wg.Wait(); cancel() }() // releases the timeout once every call has finished
	return collect(ctx, callCtx, results, len(targets), need)
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
