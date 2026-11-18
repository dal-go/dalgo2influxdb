package dalgo2influxdb

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"
	"unicode/utf8"
)

// Client owns its cloned HTTP transport and operation admission. Construction
// performs no I/O. Close cancels admitted operations; no caller transport is
// mutated or closed. Use a provider credential with read authority only.
type Client struct {
	opts      Options
	endpoint  string
	http      *http.Client
	transport *http.Transport
	base      context.Context
	cancel    context.CancelFunc
	sem       chan struct{}
	closeOnce sync.Once
}

type operation struct {
	ctx           context.Context
	requestBytes  int
	responseBytes int
	rows          int
}

func fresh(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func New(serverURL string, opts Options) (*Client, error) {
	u, err := url.Parse(serverURL)
	if err != nil || u.Opaque != "" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, ErrQuery
	}
	loopback := u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback && opts.InsecureLoopbackForTesting) {
		return nil, ErrQuery
	}
	if !validName(opts.Database) || opts.TokenProvider == nil || len(opts.Tables) == 0 || len(opts.Tables) > 128 {
		return nil, ErrQuery
	}
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.MaxRows == 0 {
		opts.MaxRows = 1000
	}
	if opts.MaxResponseBytes == 0 {
		opts.MaxResponseBytes = 4 * 1024 * 1024
	}
	if opts.MaxRequestBytes == 0 {
		opts.MaxRequestBytes = 64 * 1024
	}
	if opts.MaxConcurrent == 0 {
		opts.MaxConcurrent = 4
	}
	if opts.Timeout < time.Millisecond || opts.Timeout > 2*time.Minute || opts.MaxRows < 1 || opts.MaxRows > 10000 || opts.MaxResponseBytes < 1 || opts.MaxResponseBytes > 16*1024*1024 || opts.MaxRequestBytes < 1 || opts.MaxRequestBytes > 1024*1024 || opts.MaxConcurrent < 1 || opts.MaxConcurrent > 16 {
		return nil, ErrQuery
	}
	tables := make(map[string]Table, len(opts.Tables))
	for name, t := range opts.Tables {
		if !validName(name) || !validName(t.Name) || !validName(t.Key.Name) || t.Key.Nullable || (t.Key.Kind != "" && t.Key.Kind != "string") || len(t.Columns) > 128 {
			return nil, ErrQuery
		}
		t.Key.Kind = "string"
		cols := make(map[string]Column, len(t.Columns))
		native := map[string]bool{t.Key.Name: true}
		for field, col := range t.Columns {
			if !validName(field) || field == "__name__" || field == "__dalgo_key" || !validName(col.Name) || native[col.Name] || (col.Kind != "" && col.Kind != "string" && col.Kind != "number" && col.Kind != "boolean") {
				return nil, ErrQuery
			}
			native[col.Name] = true
			cols[field] = col
		}
		t.Columns = cols
		tables[name] = t
	}
	opts.Tables = tables
	var transport *http.Transport
	if opts.Transport != nil {
		if opts.Transport.TLSClientConfig != nil && opts.Transport.TLSClientConfig.InsecureSkipVerify {
			return nil, ErrQuery
		}
		transport = opts.Transport.Clone()
	} else {
		transport = &http.Transport{DialContext: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext}
	}
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.MaxConnsPerHost = opts.MaxConcurrent
	transport.MaxIdleConns = opts.MaxConcurrent
	transport.MaxIdleConnsPerHost = opts.MaxConcurrent
	transport.IdleConnTimeout = 30 * time.Second
	transport.ResponseHeaderTimeout = opts.Timeout
	transport.TLSHandshakeTimeout = min(10*time.Second, opts.Timeout)
	transport.MaxResponseHeaderBytes = 64 * 1024
	base, cancel := context.WithCancel(context.Background())
	u.Path = "/api/v3/query_sql"
	u.RawPath = ""
	c := &Client{opts: opts, endpoint: u.String(), transport: transport, base: base, cancel: cancel, sem: make(chan struct{}, opts.MaxConcurrent)}
	c.http = &http.Client{Transport: transport, Timeout: opts.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return c, nil
}

func (c *Client) Close() error {
	c.closeOnce.Do(func() { c.cancel(); c.transport.CloseIdleConnections() })
	return nil
}

// run holds admission until the actual worker completes, even if a token
// callback ignores cancellation. Detached work cannot silently free its lease.
func run[T any](c *Client, parent context.Context, fn func(*operation) (T, error)) (zero T, err error) {
	if err = fresh(parent); err != nil {
		return zero, err
	}
	if c.base.Err() != nil {
		return zero, ErrClosed
	}
	ctx, cancel := context.WithTimeout(parent, c.opts.Timeout)
	stop := context.AfterFunc(c.base, cancel)
	defer stop()
	defer cancel()
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return zero, ctx.Err()
	default:
		return zero, ErrBusy
	}
	type outcome struct {
		value T
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		defer func() { <-c.sem }()
		value, e := fn(&operation{ctx: ctx})
		if cancelled := fresh(ctx); cancelled != nil {
			e = cancelled
		}
		done <- outcome{value, e}
	}()
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case out := <-done:
		if e := fresh(ctx); e != nil {
			return zero, e
		}
		if out.err != nil {
			return zero, out.err
		}
		return out.value, nil
	}
}

func (c *Client) Query(ctx context.Context, q Query) (Page, error) {
	// Admission and bounded copying precede any detached execution. Once Query
	// returns, a cancelled worker cannot read caller-owned slice backing arrays.
	if len(q.Filters) > 64 || len(q.Order) > 16 || len(q.Projection) > 128 || len(q.StartAfter) > 17 {
		return Page{}, ErrQuery
	}
	q.Filters = slices.Clone(q.Filters)
	q.Order = slices.Clone(q.Order)
	q.Projection = slices.Clone(q.Projection)
	q.StartAfter = slices.Clone(q.StartAfter)
	return run(c, ctx, func(op *operation) (Page, error) {
		p, err := compile(c.opts.Tables, q, c.opts.MaxRows, c.opts.MaxRequestBytes)
		if err != nil {
			return Page{}, err
		}
		if err = fresh(op.ctx); err != nil {
			return Page{}, err
		}
		return c.execute(op, p)
	})
}

func (c *Client) execute(op *operation, p plan) (Page, error) {
	if err := fresh(op.ctx); err != nil {
		return Page{}, err
	}
	payload, err := json.Marshal(map[string]any{"db": c.opts.Database, "q": p.sql, "params": p.params, "format": "json"})
	if err != nil {
		return Page{}, ErrQuery
	}
	if len(payload) > c.opts.MaxRequestBytes-op.requestBytes {
		return Page{}, ErrBudget
	}
	op.requestBytes += len(payload)
	token, err := c.opts.TokenProvider(op.ctx)
	if e := fresh(op.ctx); e != nil {
		return Page{}, e
	}
	if err != nil {
		return Page{}, ErrTransport
	}
	if len(token) < 1 || len(token) > 8192 {
		return Page{}, ErrTransport
	}
	for _, r := range token {
		if r < 0x21 || r > 0x7e {
			return Page{}, ErrTransport
		}
	}
	requestCtx, cancel := context.WithCancel(op.ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return Page{}, ErrTransport
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	response, err := c.http.Do(req)
	if e := fresh(op.ctx); e != nil {
		if response != nil {
			cancel()
			_ = response.Body.Close()
		}
		return Page{}, e
	}
	if err != nil {
		return Page{}, ErrTransport
	}
	defer func() { cancel(); _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return Page{}, ErrTransport
	}
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" || (response.Header.Get("Content-Encoding") != "" && response.Header.Get("Content-Encoding") != "identity") {
		return Page{}, ErrResponse
	}
	remaining := c.opts.MaxResponseBytes - op.responseBytes
	if remaining < 0 || response.ContentLength > int64(remaining) {
		return Page{}, ErrBudget
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(remaining)+1))
	if e := fresh(op.ctx); e != nil {
		return Page{}, e
	}
	if err != nil {
		return Page{}, ErrTransport
	}
	if len(body) > remaining {
		return Page{}, ErrBudget
	}
	op.responseBytes += len(body)
	raw, err := decodeRows(op.ctx, body, p)
	if err != nil {
		return Page{}, err
	}
	if len(raw) > c.opts.MaxRows-op.rows+1 {
		return Page{}, ErrBudget
	}
	op.rows += len(raw)
	n := min(p.limit, len(raw))
	page := Page{Rows: make([]Row, 0, n)}
	for _, r := range raw[:n] {
		data := make(map[string]any, len(p.output))
		for _, f := range p.output {
			data[f] = r[f]
		}
		page.Rows = append(page.Rows, Row{Key: r["__dalgo_key"].(string), Data: data})
	}
	if len(raw) > p.limit {
		last := raw[p.limit-1]
		page.NextCursor = make([]any, len(p.order))
		for i, o := range p.order {
			if o.Field == "__name__" {
				page.NextCursor[i] = last["__dalgo_key"]
			} else {
				page.NextCursor[i] = last[o.Field]
			}
		}
	}
	if e := fresh(op.ctx); e != nil {
		return Page{}, e
	}
	return page, nil
}

// decodeRows accepts only the v3 JSON object-row format and rejects duplicate
// members instead of allowing encoding/json's usual last-member-wins behavior.
func decodeRows(ctx context.Context, body []byte, p plan) ([]map[string]any, error) {
	if !utf8.Valid(body) {
		return nil, ErrResponse
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	start, err := d.Token()
	if err != nil || start != json.Delim('[') {
		return nil, ErrResponse
	}
	expected := map[string]Column{"__dalgo_key": p.table.Key}
	for _, f := range p.selected {
		expected[f] = p.table.Columns[f]
	}
	var rows []map[string]any
	keys := map[string]bool{}
	for d.More() {
		if e := fresh(ctx); e != nil {
			return nil, e
		}
		if len(rows) >= p.limit+1 {
			return nil, ErrBudget
		}
		start, err = d.Token()
		if err != nil || start != json.Delim('{') {
			return nil, ErrResponse
		}
		row := make(map[string]any, len(expected))
		for d.More() {
			name, e := d.Token()
			if e != nil {
				return nil, ErrResponse
			}
			field, ok := name.(string)
			if !ok {
				return nil, ErrResponse
			}
			col, ok := expected[field]
			if !ok {
				return nil, ErrResponse
			}
			if _, duplicate := row[field]; duplicate {
				return nil, ErrResponse
			}
			value, e := d.Token()
			if e != nil {
				return nil, ErrResponse
			}
			value, kind, ok := scalar(value)
			if !ok || (kind == "null" && !col.Nullable) || (kind != "null" && col.Kind != "" && kind != col.Kind) {
				return nil, ErrResponse
			}
			row[field] = value
		}
		end, e := d.Token()
		if e != nil || end != json.Delim('}') || len(row) != len(expected) {
			return nil, ErrResponse
		}
		key, ok := row["__dalgo_key"].(string)
		if !ok || keys[key] {
			return nil, ErrResponse
		}
		keys[key] = true
		for _, f := range p.query.Filters {
			field := f.Field
			if field == "__name__" {
				field = "__dalgo_key"
			}
			if !matches(row[field], f) {
				return nil, ErrResponse
			}
		}
		rows = append(rows, row)
	}
	end, e := d.Token()
	if e != nil || end != json.Delim(']') {
		return nil, ErrResponse
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, ErrResponse
	}
	// The server must return strict deterministic order. Number comparison is
	// exact rational arithmetic; no float64 conversion participates in paging.
	for i := 1; i < len(rows); i++ {
		if e := fresh(ctx); e != nil {
			return nil, e
		}
		if compareRows(rows[i-1], rows[i], p.order) >= 0 {
			return nil, ErrResponse
		}
	}
	if len(p.query.StartAfter) > 0 {
		cursor := map[string]any{}
		for i, o := range p.order {
			f := o.Field
			if f == "__name__" {
				f = "__dalgo_key"
			}
			cursor[f] = p.query.StartAfter[i]
		}
		for _, r := range rows {
			if e := fresh(ctx); e != nil {
				return nil, e
			}
			if compareRows(cursor, r, p.order) >= 0 {
				return nil, ErrResponse
			}
		}
	}
	return rows, nil
}
