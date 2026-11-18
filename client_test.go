package dalgo2influxdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

const fixtureToken = "owned-test-secret"

func testOptions() Options {
	return Options{Database: "owned", Tables: map[string]Table{"events": {Name: "native events", Key: Column{Name: "key_id", Kind: "string"}, Columns: map[string]Column{"n": {Name: "native_n", Kind: "number"}, "text": {Name: "native_text", Kind: "string", Nullable: true}}}}, TokenProvider: func(context.Context) (string, error) { return fixtureToken, nil }}
}

func testClient(t *testing.T, h http.HandlerFunc, change func(*Options)) (*Client, *httptest.Server) {
	t.Helper()
	s := httptest.NewTLSServer(h)
	t.Cleanup(s.Close)
	o := testOptions()
	o.Transport = s.Client().Transport.(*http.Transport)
	if change != nil {
		change(&o)
	}
	c, e := New(s.URL, o)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, s
}

func jsonReply(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, body)
}

func TestVerifiedTLSNamedParametersAndExactKeyset(t *testing.T) {
	var calls atomic.Int32
	c, s := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/api/v3/query_sql" || r.URL.RawQuery != "" || r.Header.Get("Authorization") != "Bearer "+fixtureToken {
			t.Errorf("unexpected protocol request")
			w.WriteHeader(400)
			return
		}
		var payload struct {
			DB     string         `json:"db"`
			Q      string         `json:"q"`
			Params map[string]any `json:"params"`
			Format string         `json:"format"`
		}
		d := json.NewDecoder(r.Body)
		d.UseNumber()
		if e := d.Decode(&payload); e != nil {
			t.Error(e)
			w.WriteHeader(400)
			return
		}
		if payload.DB != "owned" || payload.Format != "json" || !strings.Contains(payload.Q, `FROM "native events"`) || !strings.Contains(payload.Q, `ORDER BY "native_n" ASC, "key_id" ASC LIMIT 2`) {
			t.Errorf("wrong bounded SQL: %+v", payload)
		}
		if strings.Contains(payload.Q, "' OR TRUE") || payload.Params["p0"] != "x' OR TRUE --" {
			t.Error("filter must remain a named parameter")
		}
		if calls.Add(1) == 1 {
			jsonReply(w, `[{"__dalgo_key":"a","n":9007199254740993.0001,"text":"x' OR TRUE --"},{"__dalgo_key":"b","n":9007199254740993.0002,"text":"x' OR TRUE --"}]`)
		} else {
			if payload.Params["p1"] != json.Number("9007199254740993.0001") || !strings.Contains(payload.Q, `"key_id" > $`) {
				t.Error("exact keyset not parameterized")
			}
			jsonReply(w, `[{"__dalgo_key":"b","n":9007199254740993.0002,"text":"x' OR TRUE --"}]`)
		}
	}, nil)
	q := Query{Collection: "events", Filters: []Filter{{"text", "==", "x' OR TRUE --"}}, Order: []Order{{Field: "n"}}, Limit: 1}
	p, e := c.Query(context.Background(), q)
	if e != nil {
		t.Fatal(e)
	}
	if len(p.Rows) != 1 || p.Rows[0].Key != "a" || p.Rows[0].Data["n"] != json.Number("9007199254740993.0001") || len(p.NextCursor) != 2 {
		t.Fatalf("lost native values/paging: %+v", p)
	}
	q.StartAfter = p.NextCursor
	p, e = c.Query(context.Background(), q)
	if e != nil || len(p.Rows) != 1 || p.Rows[0].Key != "b" || p.NextCursor != nil {
		t.Fatalf("second page %+v %v", p, e)
	}
	// The owned server's CA is required; no insecure TLS option is enabled.
	o := testOptions()
	untrusted, e := New(s.URL, o)
	if e != nil {
		t.Fatal(e)
	}
	defer untrusted.Close()
	if _, e = untrusted.Query(context.Background(), Query{Collection: "events"}); !errors.Is(e, ErrTransport) {
		t.Fatalf("untrusted TLS accepted: %v", e)
	}
}

func TestMalformedOverreturnAndForeignRowsFailWholeQuery(t *testing.T) {
	cases := map[string]string{
		"duplicate field":  `[{"__dalgo_key":"a","n":1,"n":2,"text":null}]`,
		"unknown field":    `[{"__dalgo_key":"a","n":1,"text":null,"secret":"no"}]`,
		"missing field":    `[{"__dalgo_key":"a","n":1}]`,
		"null nonnullable": `[{"__dalgo_key":"a","n":null,"text":null}]`,
		"nested value":     `[{"__dalgo_key":"a","n":{},"text":null}]`,
		"wrong kind":       `[{"__dalgo_key":"a","n":"1","text":null}]`,
		"duplicate key":    `[{"__dalgo_key":"a","n":1,"text":null},{"__dalgo_key":"a","n":2,"text":null}]`,
		"wrong order":      `[{"__dalgo_key":"b","n":1,"text":null},{"__dalgo_key":"a","n":2,"text":null}]`,
		"overreturn":       `[{"__dalgo_key":"a","n":1,"text":null},{"__dalgo_key":"b","n":2,"text":null},{"__dalgo_key":"c","n":3,"text":null}]`,
		"trailing":         `[] {}`,
		"huge exponent":    `[{"__dalgo_key":"a","n":1e999999,"text":null}]`,
		"invalid UTF8":     "[\xff]",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			c, _ := testClient(t, func(w http.ResponseWriter, _ *http.Request) { jsonReply(w, body) }, nil)
			p, e := c.Query(context.Background(), Query{Collection: "events", Limit: 1})
			if e == nil || len(p.Rows) != 0 {
				t.Fatalf("unsafe partial result: %+v %v", p, e)
			}
		})
	}
	t.Run("predicate", func(t *testing.T) {
		c, _ := testClient(t, func(w http.ResponseWriter, _ *http.Request) { jsonReply(w, `[{"__dalgo_key":"a","n":1,"text":null}]`) }, nil)
		p, e := c.Query(context.Background(), Query{Collection: "events", Filters: []Filter{{"n", ">", 2}}})
		if !errors.Is(e, ErrResponse) || len(p.Rows) != 0 {
			t.Fatalf("predicate violation accepted: %+v %v", p, e)
		}
	})
	t.Run("cursor", func(t *testing.T) {
		c, _ := testClient(t, func(w http.ResponseWriter, _ *http.Request) { jsonReply(w, `[{"__dalgo_key":"a","n":1,"text":null}]`) }, nil)
		if _, e := c.Query(context.Background(), Query{Collection: "events", StartAfter: []any{"b"}}); !errors.Is(e, ErrResponse) {
			t.Fatalf("backward cursor accepted: %v", e)
		}
	})
}

func TestResponseLimitsAndProtocolRefusals(t *testing.T) {
	for _, mode := range []string{"oversize", "encoding", "redirect", "type", "status"} {
		t.Run(mode, func(t *testing.T) {
			var redirected atomic.Int32
			target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { redirected.Add(1); jsonReply(w, "[]") }))
			defer target.Close()
			c, _ := testClient(t, func(w http.ResponseWriter, _ *http.Request) {
				switch mode {
				case "oversize":
					jsonReply(w, strings.Repeat(" ", 257))
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
					jsonReply(w, "[]")
				case "redirect":
					w.Header().Set("Location", target.URL)
					w.WriteHeader(307)
				case "type":
					w.Header().Set("Content-Type", "text/plain")
					_, _ = io.WriteString(w, "[]")
				case "status":
					w.WriteHeader(500)
					_, _ = io.WriteString(w, fixtureToken)
				}
			}, func(o *Options) { o.MaxResponseBytes = 256 })
			_, e := c.Query(context.Background(), Query{Collection: "events"})
			if e == nil || strings.Contains(e.Error(), fixtureToken) || redirected.Load() != 0 {
				t.Fatalf("protocol/secret boundary failed: %v redirects=%d", e, redirected.Load())
			}
		})
	}
}

func TestInvalidInputsNeverResolveTokenOrDispatch(t *testing.T) {
	var tokens, requests atomic.Int32
	c, _ := testClient(t, func(w http.ResponseWriter, _ *http.Request) { requests.Add(1); jsonReply(w, "[]") }, func(o *Options) {
		o.TokenProvider = func(context.Context) (string, error) { tokens.Add(1); return fixtureToken, nil }
		o.MaxRequestBytes = 1024
	})
	cases := []Query{
		{Collection: "unknown"}, {Collection: "events", Projection: []string{"*"}}, {Collection: "events", Projection: []string{"n", "n"}},
		{Collection: "events", Filters: []Filter{{"n", "IN", 1}}}, {Collection: "events", Filters: []Filter{{"n", "==", json.Number("1e999999")}}},
		{Collection: "events", Order: []Order{{Field: "text"}}}, {Collection: "events", StartAfter: []any{1}},
		{Collection: "events", Filters: []Filter{{"text", "==", strings.Repeat("\n", 600)}}},
	}
	for _, q := range cases {
		if _, e := c.Query(context.Background(), q); e == nil {
			t.Fatalf("invalid query accepted: %+v", q)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := c.Query(ctx, Query{Collection: "events"}); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if tokens.Load() != 0 || requests.Load() != 0 {
		t.Fatalf("invalid inputs performed I/O: token=%d request=%d", tokens.Load(), requests.Load())
	}
	for _, endpoint := range []string{"http://example.com", "https://user:secret@example.com", "https://example.com?token=secret", "https://example.com/path"} {
		if x, e := New(endpoint, testOptions()); e == nil {
			_ = x.Close()
			t.Fatalf("unsafe endpoint accepted: %s", endpoint)
		}
	}
}

func TestCallerAndDeadlineAfterDispatchAndReopen(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "caller", true: "deadline"}[deadline], func(t *testing.T) {
			entered := make(chan struct{}, 1)
			serverCancelled := make(chan struct{}, 1)
			escape := make(chan struct{})
			var calls atomic.Int32
			c, s := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 65537))
				if calls.Add(1) > 1 {
					jsonReply(w, "[]")
					return
				}
				entered <- struct{}{}
				select {
				case <-r.Context().Done():
					serverCancelled <- struct{}{}
				case <-escape:
				}
			}, func(o *Options) {
				if deadline {
					o.Timeout = 50 * time.Millisecond
				}
			})
			// Release the owned fixture even on an assertion failure; the positive
			// disconnect ACK is still required before this cleanup escape is used.
			defer close(escape)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { _, e := c.Query(ctx, Query{Collection: "events"}); result <- e }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("request never dispatched")
			}
			want := context.Canceled
			if deadline {
				want = context.DeadlineExceeded
			} else {
				cancel()
			}
			select {
			case e := <-result:
				if !errors.Is(e, want) {
					t.Fatalf("wrong cancellation: %v", e)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation blocked")
			}
			select {
			case <-serverCancelled:
			case <-time.After(time.Second):
				t.Fatal("HTTP request context not cancelled")
			}
			_ = c.Close()
			if _, e := c.Query(context.Background(), Query{Collection: "events"}); !errors.Is(e, ErrClosed) {
				t.Fatalf("closed client accepted request: %v", e)
			}
			o := testOptions()
			o.Transport = s.Client().Transport.(*http.Transport)
			reopened, e := New(s.URL, o)
			if e != nil {
				t.Fatal(e)
			}
			defer reopened.Close()
			page, e := reopened.Query(context.Background(), Query{Collection: "events"})
			if e != nil || len(page.Rows) != 0 || calls.Load() != 2 {
				t.Fatalf("reopened query failed: %+v %v calls=%d", page, e, calls.Load())
			}
		})
	}
}

func TestUncooperativeTokenProviderRetainsAdmission(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	c, _ := testClient(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("late auth must not dispatch")
		jsonReply(w, "[]")
	}, func(o *Options) {
		o.MaxConcurrent = 1
		o.TokenProvider = func(context.Context) (string, error) { close(entered); <-release; return fixtureToken, nil }
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := c.Query(ctx, Query{Collection: "events"}); done <- e }()
	<-entered
	cancel()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("auth wait blocks cancellation")
	}
	if _, e := c.Query(context.Background(), Query{Collection: "events"}); !errors.Is(e, ErrBusy) {
		t.Fatalf("detached auth freed admission: %v", e)
	}
	_ = c.Close()
}

func TestGetManyMissingDuplicateAndAggregateBudget(t *testing.T) {
	var calls atomic.Int32
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var payload struct {
			Params map[string]string `json:"params"`
		}
		if e := json.NewDecoder(r.Body).Decode(&payload); e != nil {
			t.Error(e)
		}
		if payload.Params["p0"] == "missing" {
			jsonReply(w, "[]")
			return
		}
		row := map[string]any{"__dalgo_key": payload.Params["p0"], "n": json.Number("9007199254740993"), "text": nil}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]any{row})
	}, nil)
	rows, e := c.GetMany(context.Background(), "events", []string{"a", "missing", "a"})
	if e != nil || len(rows) != 3 || rows[0].Key != "a" || rows[1] != nil || rows[2].Key != "a" || rows[0].Data["n"] != json.Number("9007199254740993") {
		t.Fatalf("batch contract %+v %v", rows, e)
	}
	if calls.Load() != 3 {
		t.Fatal("duplicate/order accounting changed")
	}
	c.opts.MaxResponseBytes = 100
	rows, e = c.GetMany(context.Background(), "events", []string{"a", "b"})
	if !errors.Is(e, ErrBudget) || rows != nil {
		t.Fatalf("partial aggregate response escaped: %+v %v", rows, e)
	}
}

type callbackRecord struct {
	record.Record
	onData func()
	onSet  func(error)
}

func (r *callbackRecord) Data() any {
	if r.onData != nil {
		r.onData()
	}
	return r.Record.Data()
}
func (r *callbackRecord) SetError(e error) record.Record {
	r.Record.SetError(e)
	if r.onSet != nil {
		r.onSet(e)
	}
	return r
}

func TestDALgoCallbacksCaptureBatchAndGuardFinalDelivery(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		var p struct {
			Params map[string]string `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&p)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]any{map[string]any{"__dalgo_key": p.Params["p0"], "n": json.Number("9007199254740993.0001"), "text": nil}})
	}, nil)
	var a, b map[string]any
	records := []record.Record{record.NewRecordWithData(record.NewKeyWithID("events", "a"), &a), record.NewRecordWithData(record.NewKeyWithID("events", "b"), &b)}
	records[0] = &callbackRecord{Record: records[0], onData: func() { records[1] = nil }}
	if e := c.DB().GetMulti(context.Background(), records); e != nil || a["n"] != json.Number("9007199254740993.0001") || b["n"] != json.Number("9007199254740993.0001") {
		t.Fatalf("batch callback lost identities/native numbers: %v %v %v", a, b, e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var target map[string]any
	r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("events", "a"), &target), onData: cancel}
	if e := c.DB().Get(ctx, r); !errors.Is(e, context.Canceled) || !errors.Is(r.Error(), context.Canceled) || target != nil {
		t.Fatalf("callback cancellation ignored: %v target=%v", e, target)
	}
	var final map[string]any
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	r = &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("events", "a"), &final)}
	r.onSet = func(e error) {
		if e == nil && final != nil {
			cancel()
		}
	}
	if e := c.DB().Get(ctx, r); !errors.Is(e, context.Canceled) || final == nil || !errors.Is(r.Error(), context.Canceled) {
		t.Fatalf("final callback cancellation lost state: %v %v", e, final)
	}
}

func TestReaderCancellationBeforeAcknowledgement(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, _ *http.Request) { jsonReply(w, `[{"__dalgo_key":"a","n":1,"text":null}]`) }, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &recordsReader{ctx: ctx, cancel: cancel, client: c, collection: "events", rows: []Row{{Key: "a", Data: map[string]any{"n": json.Number("1")}}}}
	r, e := reader.Next()
	if e != nil || r.Key().ID != "a" || reader.index != 1 {
		t.Fatalf("first delivery %v %v", r, e)
	}
	if _, e = reader.Next(); !errors.Is(e, dal.ErrNoMoreRecords) {
		t.Fatal(e)
	}
	cancel()
	if _, e = reader.Next(); !errors.Is(e, context.Canceled) || reader.index != 1 {
		t.Fatalf("late cancellation rewound acknowledged row: %v", e)
	}
	if e = reader.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = reader.Next(); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
}

func TestDALgoBlockingCallbackCannotMutateAfterReturn(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, _ *http.Request) { jsonReply(w, `[{"__dalgo_key":"a","n":1,"text":null}]`) }, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	var target map[string]any
	r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("events", "a"), &target), onData: func() { close(entered); <-release }}
	go func() { done <- c.DB().Get(ctx, r) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("record callback never entered")
	}
	cancel()
	// A caller callback cannot be preempted safely. The public call must wait
	// for it, then publish cancellation synchronously before returning.
	select {
	case e := <-done:
		close(release)
		t.Fatalf("returned with callback still active: %v", e)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("callback release did not complete")
	}
	// This read races with the old detached-delivery implementation under -race.
	if target != nil || !errors.Is(r.Error(), context.Canceled) {
		t.Fatalf("late state mutation: %v", target)
	}
}

func TestDALgoUnboundedReaderDoesNotSilentlyStopAtHundred(t *testing.T) {
	for _, count := range []int{101, 151} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				var input struct {
					Q string `json:"q"`
				}
				if e := json.NewDecoder(r.Body).Decode(&input); e != nil {
					t.Error(e)
				}
				if !strings.HasSuffix(input.Q, " LIMIT 151") {
					t.Errorf("unbounded DALgo read not capped at whole budget: %s", input.Q)
				}
				rows := make([]any, count)
				for i := range rows {
					rows[i] = map[string]any{"__dalgo_key": fmt.Sprintf("%04d", i), "n": i, "text": nil}
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(rows)
			}, func(o *Options) { o.MaxRows = 150 })
			q := dal.NewQueryBuilder(dal.From(dal.NewCollectionRef("events", "", nil))).SelectColumns()
			reader, e := c.DB().ExecuteQueryToRecordsReader(context.Background(), q)
			if count == 151 {
				if !errors.Is(e, ErrBudget) || reader != nil {
					t.Fatalf("overflow returned false EOF: %v %v", reader, e)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			defer reader.Close()
			got := 0
			for {
				r, e := reader.Next()
				if errors.Is(e, dal.ErrNoMoreRecords) {
					break
				}
				if e != nil {
					t.Fatal(e)
				}
				if r.Key().ID != fmt.Sprintf("%04d", got) {
					t.Fatal("wrong row order")
				}
				got++
			}
			if got != count {
				t.Fatalf("silent truncation: got%d want%d", got, count)
			}
		})
	}
}
