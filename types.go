// Package dalgo2influxdb provides bounded reads of explicitly mapped InfluxDB 3 tables.
package dalgo2influxdb

import (
	"context"
	"errors"
	"net/http"
	"time"
)

var (
	ErrClosed    = errors.New("influxdb: client is closed")
	ErrBusy      = errors.New("influxdb: operation admission limit reached")
	ErrQuery     = errors.New("influxdb: unsupported or invalid mapped query")
	ErrBudget    = errors.New("influxdb: operation budget exceeded")
	ErrResponse  = errors.New("influxdb: invalid query response")
	ErrTransport = errors.New("influxdb: request failed")
)

// Column maps a logical field to a native column. Kind is string, number,
// boolean, or empty for scalar JSON values. Nullable order columns cannot be
// used for keyset paging. JSON number lexemes are retained as json.Number;
// this does not promise precision the server did not return.
type Column struct {
	Name     string
	Kind     string
	Nullable bool
}

// Table is trusted application configuration. Key must identify a unique,
// non-null string column. Composite identities and automatic discovery are
// not implemented. Columns never contain SQL expressions.
type Table struct {
	Name    string
	Key     Column
	Columns map[string]Column
}

// Options configures one client. TokenProvider is called under the whole
// operation deadline, including GetMulti, and must return a scoped read token.
// An uncooperative provider keeps its admission lease until it actually returns.
type Options struct {
	Database         string
	Tables           map[string]Table
	TokenProvider    func(context.Context) (string, error)
	Transport        *http.Transport
	Timeout          time.Duration
	MaxRows          int
	MaxResponseBytes int
	MaxRequestBytes  int
	MaxConcurrent    int
	// InsecureLoopbackForTesting admits only literal loopback HTTP endpoints.
	// Production endpoints always require verified HTTPS.
	InsecureLoopbackForTesting bool
}

// Filter is one scalar comparison. All filters are joined with AND. Supported
// operators are ==, !=, <, <=, >, >=. __name__ refers to the mapped string key.
type Filter struct {
	Field    string
	Operator string
	Value    any
}

type Order struct {
	Field      string
	Descending bool
}

// Query is a bounded mapped query, never raw SQL. StartAfter must have exactly
// one scalar per effective order (including the appended key tie breaker).
type Query struct {
	Collection string
	Projection []string
	Filters    []Filter
	Order      []Order
	Limit      int
	StartAfter []any
}

type Row struct {
	Key  string
	Data map[string]any
}

// Page is not a transaction or snapshot across requests. NextCursor is present
// only after a lookahead row proves another page exists.
type Page struct {
	Rows       []Row
	NextCursor []any
}
