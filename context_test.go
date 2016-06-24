// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package openctx

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"golang.org/x/net/context"
)

// Tests the low-level baggage interface directly.
func TestWithBaggage(t *testing.T) {
	ctx := context.Background()
	ctx = WithBaggage(ctx, "TTL", "1000")
	ttl, ok := Baggage(ctx, "TTL")
	assert.True(t, ok)
	assert.Equal(t, "1000", ttl)
	assert.Equal(t, []string{"ttl"}, Keys(ctx))
}

// Third-party libraries can introduce X(ctx) and WithX(ctx, x) functions for
// the type and merge behavior for particular baggage properties.  This tests
// an example third-party baggage property for TTL, which in turn exercises
// WithBaggageJoin and Baggage. When joining two TTLs, the smaller should be taken.

func TTL(ctx context.Context) (time.Duration, bool) {
	ttl, ok := Baggage(ctx, "TTL")
	if !ok {
		return 0, false
	}
	ms, err := strconv.Atoi(ttl)
	if err != nil {
		return 0, false
	}
	return time.Duration(ms) * time.Millisecond, true
}

func WithTTL(ctx context.Context, TTL time.Duration) context.Context {
	return WithBaggageJoin(ctx, "TTL", fmt.Sprintf("%d", TTL/time.Millisecond), joinTTL)
}

func joinTTL(a, b string) string {
	attl, err := strconv.Atoi(a)
	if err != nil {
		return b
	}
	bttl, err := strconv.Atoi(b)
	if err != nil {
		return "0"
	}
	if attl < bttl {
		return a
	}
	return b
}

func TestJoinWithLessTTL(t *testing.T) {
	ctx := context.Background()
	ctx = WithTTL(ctx, time.Second)
	ctx = WithTTL(ctx, 100*time.Millisecond)
	ttl, ok := TTL(ctx)
	assert.True(t, ok)
	assert.Equal(t, 100*time.Millisecond, ttl)
	assert.Equal(t, []string{"ttl"}, Keys(ctx))
}

func TestJoinWithMoreTTL(t *testing.T) {
	ctx := context.Background()
	ctx = WithTTL(ctx, time.Second)
	ctx = WithTTL(ctx, 10*time.Second)
	ttl, ok := TTL(ctx)
	assert.True(t, ok)
	assert.Equal(t, time.Second, ttl)
	assert.Equal(t, []string{"ttl"}, Keys(ctx))
}

// Another third party library provides an interface for Receipts baggage.
// Receipts is a sorted set of all services that have participated in
// processing a request.

func WithReceipt(ctx context.Context, receipt string) context.Context {
	return WithBaggageJoin(ctx, "Receipts", receipt, joinReceipts)
}

func Receipts(ctx context.Context) []string {
	receipts, ok := Baggage(ctx, "Receipts")
	if !ok {
		return []string{}
	}
	return strings.Split(receipts, ", ")
}

func joinReceipts(a, b string) string {
	as := strings.Split(a, ", ")
	bs := strings.Split(b, ", ")
	set := make(map[string]struct{}, len(as)+len(bs))
	for _, receipt := range as {
		set[receipt] = struct{}{}
	}
	for _, receipt := range bs {
		set[receipt] = struct{}{}
	}
	var receipts []string
	for receipt := range set {
		receipts = append(receipts, receipt)
	}
	sort.Strings(receipts)
	return strings.Join(receipts, ", ")
}

func TestJoinReceipts(t *testing.T) {
	assert.Equal(t, "a, b", joinReceipts("a", "b"))
	assert.Equal(t, "a, b, c, d", joinReceipts("a, c", "b, d"))
}
func TestWithReceipt(t *testing.T) {
	ctx := context.Background()
	ctx = WithReceipt(ctx, "alice")
	ctx = WithReceipt(ctx, "bob")
	assert.Equal(t, []string{"alice", "bob"}, Receipts(ctx))
	assert.Equal(t, []string{"receipts"}, Keys(ctx))
}

// For the following tests, we emulate RPC for services "alice", "bob",
// "charlie", "danny", and "elizabeth".

// Alice is a service that calls bob twice serially  and performs some
// additional processing.  Alice and bob both annotate receipts, so receipts
// from alice include both "alice" and "bob".

func alice(ctx context.Context) context.Context {
	ctx = WithReceipt(ctx, "alice")
	ctx = bob(ctx)
	ctx = bob(ctx)
	return ctx
}

func bob(ctx context.Context) context.Context {
	ctx = WithReceipt(ctx, "bob")
	return ctx
}

func TestAliceCallsBob(t *testing.T) {
	ctx := context.Background()
	ctx = WithTTL(ctx, time.Second)
	ctx = alice(ctx)
	assert.Equal(t, []string{"alice", "bob"}, Receipts(ctx))
	assert.Equal(t, []string{"receipts", "ttl"}, Keys(ctx))
}

// Charlie is a more elaborate service that sends requests in parallel to
// Alice, Danny, and Elizabeth. Alice in turn entrains Bob.  Since these
// requests are requested in parallel, they may not respond in order and the
// caller is responsible for ensuring that all of the response contexts are
// joined with the prior context, responding eventually with the fully
// aggregated response context.

func charlie(ctx context.Context, t *testing.T) context.Context {
	ctx = WithReceipt(ctx, "charlie")
	assert.Equal(t, []string{"charlie"}, Receipts(ctx), "charlie inbound")

	// issue three parallel requests

	ctxB := alice(ctx)
	assert.Equal(t, []string{"alice", "bob", "charlie"}, Receipts(ctxB), "bob response")

	ctxD := danny(ctx)
	assert.Equal(t, []string{"charlie", "danny"}, Receipts(ctxD), "danny response")

	ctxE := elizabeth(ctx)
	assert.Equal(t, []string{"charlie", "elizabeth"}, Receipts(ctxE), "elizabeth response")

	// join the resulting contexts in the order the responses are received

	ctx = Join(ctx, ctxD)
	assert.Equal(t, []string{"charlie", "danny"}, Receipts(ctx), "first join")

	ctx = Join(ctx, ctxE)
	assert.Equal(t, []string{"charlie", "danny", "elizabeth"}, Receipts(ctx), "second join")

	ctx = Join(ctx, ctxB)
	assert.Equal(t, []string{"alice", "bob", "charlie", "danny", "elizabeth"}, Receipts(ctx), "third join")

	return ctx
}

func danny(ctx context.Context) context.Context {
	ctx = WithReceipt(ctx, "danny")
	return ctx
}

func elizabeth(ctx context.Context) context.Context {
	ctx = WithReceipt(ctx, "elizabeth")
	return ctx
}

func TestCharlieCallsEveryone(t *testing.T) {
	ctx := context.Background()
	ctx = WithJoin(ctx, "ttl", joinTTL)
	ctx = WithJoin(ctx, "receipts", joinReceipts)
	ctx = WithTTL(ctx, time.Second)
	ctx = charlie(ctx, t)
}
