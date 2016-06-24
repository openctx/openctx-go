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

// Open Context facilitates carrying distributed context, or "baggage", in the
// form of key value string pairs. In the context of RPC, baggage gets carried
// over the wire by a transport protocol over both requests and responses. Both
// serial and parallel request and response contexts can be merged. Recognized
// baggage can be merged with "join" functions with specialized behavior for
// each property name. For example "TTL" baggage can be merged by taking the
// minimum of joined TTLs. Another example, "Receipts" baggage can be merged by
// taking the union of combined receipts. Tracing and logical clocks are other
// uses for baggage. RPC and Trace transports can specialize behavior for
// individual properties, for example, serializing tracing only for outbound
// requests, storing TTL as a deadline in process memory relative to time of
// receipt, and serializing miscelaneous headers with a prefix on the transport
// headers.
//
// Open Context carries baggage directly on the Go context object, as well as
// optionally carrying a map of join functions for baggage property names.

package openctx

import (
	"sort"
	"strings"

	"golang.org/x/net/context"
)

// Baggage is carried on a context map by a baggage key, uniquely identified by
// both its string value and this hidden type alias.
type baggageKey string

// Join functions are also carried on a context map using a join key.
type joinKey string

// Instead of tracking keys as a set on every context object, we expect baggage
// keys to converge globally on a small set. We determine which keys are on a
// context by enumerating all known keys and filtering for the keys actually
// encountered on the context.
var knownKeys map[baggageKey]struct{}

func learnKey(key baggageKey) {
	if knownKeys == nil {
		knownKeys = make(map[baggageKey]struct{}, 10)
	}
	knownKeys[key] = struct{}{}
}

// WithBaggage adds a baggage value for a key and returns a new context,
// joining the value with any prior known value, or taking the latter if there
// is no appropriate joiner in context.
func WithBaggage(ctx context.Context, key, value string) context.Context {
	key = strings.ToLower(key)
	bkey := baggageKey(key)
	jkey := joinKey(key)
	join := ctx.Value(jkey)
	if join != nil {
		return withBaggageJoin(ctx, bkey, value, join.(func(a, b string) string))
	}
	learnKey(bkey)
	return context.WithValue(ctx, bkey, value)
}

func withBaggage(ctx context.Context, bkey baggageKey, value string) context.Context {
	jkey := joinKey(bkey)
	join := ctx.Value(jkey)
	if join != nil {
		return withBaggageJoin(ctx, bkey, value, join.(func(a, b string) string))
	}
	return context.WithValue(ctx, bkey, value)
}

// WithBaggageJoin either adds or merges a baggage value with a given join
// function and returns a new context.
func WithBaggageJoin(ctx context.Context, key, value string, join func(a, b string) string) context.Context {
	bkey := baggageKey(strings.ToLower(key))
	return withBaggageJoin(ctx, bkey, value, join)
}

// The internal withBaggageJoin method accepts the typed baggage key and
// returns a new context with the joined baggage.
func withBaggageJoin(ctx context.Context, bkey baggageKey, value string, join func(a, b string) string) context.Context {
	prior := ctx.Value(bkey)
	if prior != nil {
		value = join(prior.(string), value)
	}
	learnKey(bkey)
	return context.WithValue(ctx, bkey, value)
}

// Baggage returns the value for a given baggage key.
func Baggage(ctx context.Context, key string) (value string, ok bool) {
	bkey := baggageKey(strings.ToLower(key))
	bval := ctx.Value(bkey)
	if bval != nil {
		return bval.(string), true
	}
	return "", false
}

// Keys returns the baggage key names carried by a context.
// This method is intended for exclusively for the use of baggage serializers.
func Keys(ctx context.Context) []string {
	keys := []string{}
	for bkey := range knownKeys {
		val := ctx.Value(bkey)
		if val != nil {
			keys = append(keys, string(bkey))
		}
	}
	sort.Strings(keys)
	return keys
}

// WithJoin introduces a join function for a baggage property in the current
// context.  This would typically be called by an RPC library to ensure that
// keys with known semantics merge properly from subsequent response contexts.
func WithJoin(ctx context.Context, key string, join func(a, b string) string) context.Context {
	jkey := joinKey(strings.ToLower(key))
	return context.WithValue(ctx, jkey, join)
}

// Join two contexts, using given merge functions for known keys, otherwise
// taking baggage from the later context when there are conflicts.
func Join(this context.Context, that context.Context) context.Context {
	for bkey := range knownKeys {
		val := that.Value(bkey)
		if val != nil {
			this = withBaggage(this, bkey, val.(string))
		}
	}
	return this
}
