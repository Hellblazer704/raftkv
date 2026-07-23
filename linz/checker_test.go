package linz

import (
	"math"
	"testing"
	"time"
)

func check(t *testing.T, ops []Op, want bool) {
	t.Helper()
	res := CheckKV(ops, 10*time.Second)
	if res.Unknown {
		t.Fatal("checker timed out on a tiny history")
	}
	if res.Linearizable != want {
		t.Fatalf("linearizable=%v, want %v (bad key %q)", res.Linearizable, want, res.BadKey)
	}
}

func TestSequential(t *testing.T) {
	check(t, []Op{
		{Client: 0, Kind: Put, Key: "x", Value: "1", Call: 0, Return: 10},
		{Client: 0, Kind: Get, Key: "x", Output: "1", Call: 20, Return: 30},
		{Client: 0, Kind: Append, Key: "x", Value: "2", Call: 40, Return: 50},
		{Client: 0, Kind: Get, Key: "x", Output: "12", Call: 60, Return: 70},
	}, true)
}

func TestStaleReadViolation(t *testing.T) {
	// c2 observes the new value, then c3 observes the old one strictly later:
	// no linearization order explains both.
	check(t, []Op{
		{Client: 1, Kind: Put, Key: "x", Value: "new", Call: 0, Return: 10},
		{Client: 2, Kind: Get, Key: "x", Output: "new", Call: 20, Return: 30},
		{Client: 3, Kind: Get, Key: "x", Output: "", Call: 40, Return: 50},
	}, false)
}

func TestConcurrentPutsEitherOrder(t *testing.T) {
	// Two overlapping puts: a read may see either winner...
	base := []Op{
		{Client: 1, Kind: Put, Key: "x", Value: "a", Call: 0, Return: 100},
		{Client: 2, Kind: Put, Key: "x", Value: "b", Call: 0, Return: 100},
	}
	check(t, append(base, Op{Client: 3, Kind: Get, Key: "x", Output: "a", Call: 200, Return: 210}), true)
	check(t, append(base, Op{Client: 3, Kind: Get, Key: "x", Output: "b", Call: 200, Return: 210}), true)
	// ...but not a value nobody wrote.
	check(t, append(base, Op{Client: 3, Kind: Get, Key: "x", Output: "c", Call: 200, Return: 210}), false)
}

func TestReadInsideWriteWindow(t *testing.T) {
	// A read overlapping a put may see old or new.
	check(t, []Op{
		{Client: 1, Kind: Put, Key: "x", Value: "v", Call: 0, Return: 100},
		{Client: 2, Kind: Get, Key: "x", Output: "", Call: 10, Return: 20},
		{Client: 3, Kind: Get, Key: "x", Output: "v", Call: 30, Return: 40},
	}, true)
	// But once observed, it cannot un-happen.
	check(t, []Op{
		{Client: 1, Kind: Put, Key: "x", Value: "v", Call: 0, Return: 100},
		{Client: 2, Kind: Get, Key: "x", Output: "v", Call: 10, Return: 20},
		{Client: 3, Kind: Get, Key: "x", Output: "", Call: 30, Return: 40},
	}, false)
}

func TestIndeterminateWriteMayLandLate(t *testing.T) {
	// The put timed out; it may take effect after the first read.
	check(t, []Op{
		{Client: 1, Kind: Put, Key: "x", Value: "v", Call: 0, Return: math.MaxInt64},
		{Client: 2, Kind: Get, Key: "x", Output: "", Call: 100, Return: 110},
		{Client: 3, Kind: Get, Key: "x", Output: "v", Call: 200, Return: 210},
	}, true)
	// Or never take effect (it linearizes after the final read).
	check(t, []Op{
		{Client: 1, Kind: Put, Key: "x", Value: "v", Call: 0, Return: math.MaxInt64},
		{Client: 2, Kind: Get, Key: "x", Output: "", Call: 100, Return: 110},
	}, true)
	// But it cannot be observed and then unobserved.
	check(t, []Op{
		{Client: 1, Kind: Put, Key: "x", Value: "v", Call: 0, Return: math.MaxInt64},
		{Client: 2, Kind: Get, Key: "x", Output: "v", Call: 100, Return: 110},
		{Client: 3, Kind: Get, Key: "x", Output: "", Call: 200, Return: 210},
	}, false)
}

func TestAppendOrderObserved(t *testing.T) {
	// Reads pin the order of concurrent appends; a later read contradicting
	// that order is a violation.
	base := []Op{
		{Client: 1, Kind: Append, Key: "x", Value: "a", Call: 0, Return: 100},
		{Client: 2, Kind: Append, Key: "x", Value: "b", Call: 0, Return: 100},
	}
	check(t, append(base,
		Op{Client: 3, Kind: Get, Key: "x", Output: "ab", Call: 200, Return: 210},
		Op{Client: 4, Kind: Get, Key: "x", Output: "ab", Call: 300, Return: 310},
	), true)
	check(t, append(base,
		Op{Client: 3, Kind: Get, Key: "x", Output: "ab", Call: 200, Return: 210},
		Op{Client: 4, Kind: Get, Key: "x", Output: "ba", Call: 300, Return: 310},
	), false)
}

func TestKeysCheckedIndependently(t *testing.T) {
	// A violation on one key is found even when other keys are clean.
	res := CheckKV([]Op{
		{Client: 1, Kind: Put, Key: "good", Value: "1", Call: 0, Return: 10},
		{Client: 2, Kind: Get, Key: "good", Output: "1", Call: 20, Return: 30},
		{Client: 1, Kind: Put, Key: "bad", Value: "1", Call: 0, Return: 10},
		{Client: 2, Kind: Get, Key: "bad", Output: "wrong", Call: 20, Return: 30},
	}, 10*time.Second)
	if res.Linearizable || res.BadKey != "bad" {
		t.Fatalf("got %+v", res)
	}
}

func TestConcurrentSoup(t *testing.T) {
	// A denser interleaving that requires actual search: 3 writers, 3
	// readers, all overlapping, with a consistent explanation.
	check(t, []Op{
		{Client: 1, Kind: Put, Key: "x", Value: "a", Call: 0, Return: 50},
		{Client: 2, Kind: Append, Key: "x", Value: "b", Call: 10, Return: 60},
		{Client: 3, Kind: Append, Key: "x", Value: "c", Call: 20, Return: 70},
		{Client: 4, Kind: Get, Key: "x", Output: "abc", Call: 65, Return: 90},
		{Client: 5, Kind: Get, Key: "x", Output: "abc", Call: 95, Return: 100},
		{Client: 6, Kind: Get, Key: "x", Output: "", Call: 0, Return: 5},
	}, true)
}
