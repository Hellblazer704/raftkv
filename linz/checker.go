// Package linz is a linearizability checker for key-value histories,
// implementing the Wing & Gong / Lowe algorithm (the same family as Knossos
// and porcupine): a depth-first search over possible linearization orders
// with memoization on (set-of-linearized-ops, state).
//
// Histories are checked per key (P-compositionality: operations on distinct
// keys commute, and linearizability is compositional — Herlihy & Wing §3.3),
// which turns one exponential search into many small ones.
//
// Indeterminate operations (client timed out; the op may or may not have
// taken effect, at any later time) are modeled with Return = MaxInt64. They
// must still linearize somewhere, which is sound for writes: a write that in
// reality never landed can always be placed after the last observation.
package linz

import (
	"fmt"
	"math"
	"sort"
	"time"
)

type Kind uint8

const (
	Get Kind = iota
	Put
	Append
)

func (k Kind) String() string {
	switch k {
	case Get:
		return "get"
	case Put:
		return "put"
	case Append:
		return "append"
	}
	return "?"
}

// Op is one client operation with its real-time interval.
type Op struct {
	Client int
	Kind   Kind
	Key    string
	Value  string // written value (Put/Append)
	Output string // observed value (Get)
	Call   int64  // invocation time (ns)
	Return int64  // completion time (ns); math.MaxInt64 if indeterminate
}

func (o Op) String() string {
	switch o.Kind {
	case Get:
		return fmt.Sprintf("c%d get(%q)=%q [%d,%d]", o.Client, o.Key, o.Output, o.Call, o.Return)
	default:
		return fmt.Sprintf("c%d %s(%q,%q) [%d,%d]", o.Client, o.Kind, o.Key, o.Value, o.Call, o.Return)
	}
}

// Result is the checker's verdict for one history.
type Result struct {
	Linearizable bool
	// Unknown is set when the time budget expired before the search finished;
	// Linearizable is then meaningless.
	Unknown bool
	// BadKey names the first key whose sub-history failed.
	BadKey string
}

// CheckKV verifies a KV history, spending at most timeout on the whole check
// (0 means no limit).
func CheckKV(ops []Op, timeout time.Duration) Result {
	byKey := make(map[string][]Op)
	for _, op := range ops {
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	// Deterministic key order so failures reproduce identically.
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		ok, unknown := checkKey(byKey[k], deadline)
		if unknown {
			return Result{Unknown: true, BadKey: k}
		}
		if !ok {
			return Result{Linearizable: false, BadKey: k}
		}
	}
	return Result{Linearizable: true}
}

// event is a call or return point in the interleaved timeline.
type event struct {
	op       *Op
	id       int
	isReturn bool
	time     int64

	prev, next *event
	match      *event // for a call event, its return event
}

// checkKey runs Wing&Gong/Lowe DFS over a single key's operations.
func checkKey(ops []Op, deadline time.Time) (ok, unknown bool) {
	n := len(ops)
	if n == 0 {
		return true, false
	}
	events := make([]*event, 0, 2*n)
	for i := range ops {
		call := &event{op: &ops[i], id: i, time: ops[i].Call}
		ret := &event{op: &ops[i], id: i, isReturn: true, time: ops[i].Return}
		call.match = ret
		events = append(events, call, ret)
	}
	// Calls sort before returns at the same instant. Two reasons, both
	// learned the hard way (see BUGS.md): a coarse clock (Windows ticks at
	// ~0.5ms) records many ops as zero-duration points, and returns-first
	// would place such an op's return before its own call, corrupting the
	// event list; and for *different* ops with equal timestamps the real
	// order is unknowable, so treating them as concurrent is the only choice
	// that can't manufacture a false violation.
	sort.SliceStable(events, func(a, b int) bool {
		if events[a].time != events[b].time {
			return events[a].time < events[b].time
		}
		return !events[a].isReturn && events[b].isReturn
	})
	head := &event{time: math.MinInt64} // sentinel
	prev := head
	for _, e := range events {
		prev.next = e
		e.prev = prev
		prev = e
	}

	type frame struct {
		e     *event
		state string
	}
	var (
		stack      []frame
		linearized = newBitset(n)
		state      = ""
		cache      = map[string]struct{}{}
		entry      = head.next
		checked    int
	)
	// seen records (linearized-set, state) pairs; revisiting one means this
	// branch was already explored.
	seen := func(bits bitset, st string) bool {
		key := bits.key(st)
		if _, dup := cache[key]; dup {
			return true
		}
		cache[key] = struct{}{}
		return false
	}
	lift := func(e *event) {
		e.prev.next = e.next
		e.next.prev = e.prev
		r := e.match
		r.prev.next = r.next
		if r.next != nil {
			r.next.prev = r.prev
		}
	}
	unlift := func(e *event) {
		r := e.match
		r.prev.next = r
		if r.next != nil {
			r.next.prev = r
		}
		e.prev.next = e
		e.next.prev = e
	}

	for head.next != nil {
		checked++
		if checked%4096 == 0 && !deadline.IsZero() && time.Now().After(deadline) {
			return false, true
		}
		if !entry.isReturn {
			newState, legal := step(state, entry.op)
			if legal {
				linearized.set(entry.id)
				if !seen(linearized, newState) {
					stack = append(stack, frame{entry, state})
					state = newState
					lift(entry)
					entry = head.next
					continue
				}
				linearized.clear(entry.id)
			}
			entry = entry.next
			continue
		}
		// Hit a return event: nothing further can be linearized before this
		// point — backtrack.
		if len(stack) == 0 {
			return false, false
		}
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		state = top.state
		linearized.clear(top.e.id)
		unlift(top.e)
		entry = top.e.next
	}
	return true, false
}

// bitset tracks which op ids are linearized on the current DFS path.
type bitset []uint64

func newBitset(n int) bitset { return make(bitset, (n+63)/64) }

func (b bitset) set(i int)   { b[i/64] |= 1 << (i % 64) }
func (b bitset) clear(i int) { b[i/64] &^= 1 << (i % 64) }

// key packs the bitset and state into a cache key.
func (b bitset) key(state string) string {
	buf := make([]byte, 0, len(b)*8+1+len(state))
	for _, w := range b {
		buf = append(buf,
			byte(w), byte(w>>8), byte(w>>16), byte(w>>24),
			byte(w>>32), byte(w>>40), byte(w>>48), byte(w>>56))
	}
	buf = append(buf, 0)
	buf = append(buf, state...)
	return string(buf)
}

// step is the sequential KV specification.
func step(state string, op *Op) (string, bool) {
	switch op.Kind {
	case Put:
		return op.Value, true
	case Append:
		return state + op.Value, true
	case Get:
		return state, op.Output == state
	}
	return state, false
}
