package dns

import (
	"context"
	"sync"
	"time"
)

const sharedQueryTimeout = 8 * time.Second

// A query belongs to the nameserver, not to its first waiter. The last waiter
// leaving cancels it; a nameserver budget also bounds continuously joined work.
type sharedQuery struct {
	ctx     context.Context
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	result  result
}

type queryGroup struct {
	sync.Mutex
	queries map[string]*sharedQuery
	closed  bool
}

func (g *queryGroup) join(ctx context.Context, key string, timeout time.Duration, run func(context.Context) result) (*sharedQuery, func()) {
	g.Lock()
	defer g.Unlock()
	if g.closed {
		q := &sharedQuery{done: make(chan struct{}), result: result{error: context.Canceled}}
		close(q.done)
		return q, func() {}
	}
	q := g.queries[key]
	if q != nil && q.ctx.Err() != nil {
		delete(g.queries, key)
		q = nil
	}
	if q == nil {
		queryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		q = &sharedQuery{ctx: queryCtx, done: make(chan struct{}), cancel: cancel}
		if g.queries == nil {
			g.queries = make(map[string]*sharedQuery)
		}
		g.queries[key] = q
		go func() {
			defer cancel()
			q.result = run(queryCtx)
			g.Lock()
			if g.queries[key] == q {
				delete(g.queries, key)
			}
			close(q.done)
			g.Unlock()
		}()
	}
	q.waiters++
	return q, func() {
		g.Lock()
		defer g.Unlock()
		q.waiters--
		if q.waiters == 0 {
			if g.queries[key] == q {
				delete(g.queries, key)
			}
			q.cancel()
		}
	}
}

func (q *sharedQuery) wait(ctx context.Context) result {
	// Prefer a completed family even if the other family used up the budget.
	select {
	case <-q.done:
		return q.result
	default:
	}
	select {
	case <-q.done:
		return q.result
	case <-ctx.Done():
		select {
		case <-q.done:
			return q.result
		default:
			return result{error: ctx.Err()}
		}
	}
}

func (g *queryGroup) close() {
	g.Lock()
	defer g.Unlock()
	g.closed = true
	for key, q := range g.queries {
		delete(g.queries, key)
		q.cancel()
	}
}
