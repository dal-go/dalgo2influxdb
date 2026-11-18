package dalgo2influxdb

import (
	"context"
	"maps"
	"slices"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

func (c *Client) readKey(op *operation, collection, key string) (*Row, error) {
	if len(key) > c.opts.MaxRequestBytes {
		return nil, ErrBudget
	}
	q := Query{Collection: collection, Filters: []Filter{{Field: "__name__", Operator: "==", Value: key}}, Limit: 1}
	p, e := compile(c.opts.Tables, q, c.opts.MaxRows, c.opts.MaxRequestBytes)
	if e != nil {
		return nil, e
	}
	page, e := c.execute(op, p)
	if e != nil {
		return nil, e
	}
	if len(page.Rows) == 0 {
		return nil, nil
	}
	if len(page.Rows) != 1 || len(page.NextCursor) > 0 || page.Rows[0].Key != key {
		return nil, ErrResponse
	}
	return &page.Rows[0], nil
}

// Get returns nil for a missing string key in the configured collection.
func (c *Client) Get(ctx context.Context, collection, key string) (*Row, error) {
	return run(c, ctx, func(op *operation) (*Row, error) { return c.readKey(op, collection, key) })
}

func (c *Client) Exists(ctx context.Context, collection, key string) (bool, error) {
	return run(c, ctx, func(op *operation) (bool, error) { r, e := c.readKey(op, collection, key); return r != nil, e })
}

// GetMany preserves requested order and duplicates. Each read shares the
// original operation deadline and aggregate bytes/rows/request budget.
func (c *Client) GetMany(ctx context.Context, collection string, keys []string) ([]*Row, error) {
	if len(keys) > c.opts.MaxRows {
		return nil, ErrBudget
	}
	keys = slices.Clone(keys)
	return run(c, ctx, func(op *operation) ([]*Row, error) {
		for _, key := range keys {
			if len(key) > c.opts.MaxRequestBytes {
				return nil, ErrBudget
			}
		}
		rows := make([]*Row, 0, len(keys))
		for _, key := range keys {
			if e := fresh(op.ctx); e != nil {
				return nil, e
			}
			r, e := c.readKey(op, collection, key)
			if e != nil {
				return nil, e
			}
			rows = append(rows, r)
		}
		return rows, nil
	})
}

type backend struct {
	dal.NoConcurrency
	c *Client
}

var _ dal.Backend = (*backend)(nil)

func (c *Client) DB() dal.DB            { return dal.NewDB(&backend{c: c}) }
func (b *backend) ID() string           { return b.c.opts.Database }
func (b *backend) Adapter() dal.Adapter { return dal.NewAdapter("dalgo2influxdb", "0.1.0") }
func (b *backend) Schema() dal.Schema   { return nil }
func (b *backend) RunReadonlyTransaction(context.Context, dal.ROTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (b *backend) RunReadwriteTransaction(context.Context, dal.RWTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (b *backend) ExecuteQueryToRecordsetReader(context.Context, dal.Query, ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, dal.ErrNotSupported
}
func (b *backend) Get(ctx context.Context, r record.Record) error {
	return b.GetMulti(ctx, []record.Record{r})
}
func (b *backend) Exists(ctx context.Context, k *record.Key) (bool, error) {
	if k == nil || k.Parent() != nil {
		return false, dal.ErrNotSupported
	}
	id, ok := k.ID.(string)
	if !ok {
		return false, dal.ErrNotSupported
	}
	return b.c.Exists(ctx, k.Collection(), id)
}

func (b *backend) GetMulti(ctx context.Context, records []record.Record) error {
	if len(records) > b.c.opts.MaxRows {
		return ErrBudget
	}
	records = slices.Clone(records)
	readCtx, cancel := context.WithTimeout(ctx, b.c.opts.Timeout)
	defer cancel()
	check := func() error {
		if e := fresh(readCtx); e != nil {
			return e
		}
		if b.c.base.Err() != nil {
			return ErrClosed
		}
		return nil
	}
	type identity struct {
		key            *record.Key
		collection, id string
	}
	ids := make([]identity, len(records))
	// Caller-owned identity callbacks stay on this goroutine. They cannot be
	// safely preempted; recheck deadline/Close immediately after each returns.
	for i, r := range records {
		if e := check(); e != nil {
			return e
		}
		if r == nil {
			return dal.ErrNotSupported
		}
		k := r.Key()
		if e := check(); e != nil {
			return e
		}
		if k == nil || k.Parent() != nil {
			return dal.ErrNotSupported
		}
		id, ok := k.ID.(string)
		if !ok || len(id) > b.c.opts.MaxRequestBytes {
			return dal.ErrNotSupported
		}
		collection := k.Collection()
		if _, ok := b.c.opts.Tables[collection]; !ok {
			return ErrQuery
		}
		ids[i] = identity{record.NewKeyWithID(collection, id), collection, id}
	}
	// Only transport/planning and adapter-owned data run asynchronously. No
	// caller record or callback is reachable from the detached HTTP worker.
	rows, err := run(b.c, readCtx, func(op *operation) ([]*Row, error) {
		result := make([]*Row, 0, len(ids))
		for _, id := range ids {
			row, e := b.c.readKey(op, id.collection, id.id)
			if e != nil {
				return nil, e
			}
			result = append(result, row)
		}
		return result, nil
	})
	if err != nil {
		if len(records) > 0 {
			records[0].SetError(err)
			if e := check(); e != nil {
				records[0].SetError(e)
				return e
			}
		}
		return err
	}
	for i, r := range records {
		if e := check(); e != nil {
			r.SetError(e)
			return e
		}
		var e error
		if rows[i] == nil {
			e = dal.NewErrNotFoundByKey(ids[i].key, nil)
		} else {
			r.SetError(nil)
			if e = check(); e == nil {
				data := r.Data()
				if e = check(); e == nil {
					e = mapToTarget(data, rows[i].Data)
				}
			}
		}
		r.SetError(e)
		if cancelled := check(); cancelled != nil {
			r.SetError(cancelled)
			return cancelled
		}
		if e != nil {
			return e
		}
	}
	return check()
}

// A pointer-to-map must retain native json.Number values instead of going
// through record.MapToData's general JSON-to-struct fallback (float64 for any).
func mapToTarget(target any, values map[string]any) error {
	switch target := target.(type) {
	case map[string]any:
		if target == nil {
			return ErrResponse
		}
		maps.Copy(target, values)
		return nil
	case *map[string]any:
		if target == nil {
			return ErrResponse
		}
		if *target == nil {
			*target = make(map[string]any, len(values))
		}
		maps.Copy(*target, values)
		return nil
	default:
		if err := record.MapToData(target, values); err != nil {
			return ErrResponse
		}
		return nil
	}
}

func mappedQuery(query dal.Query) (Query, error) {
	q, ok := query.(dal.StructuredQuery)
	if !ok || q.From() == nil || q.From().Base() == nil || len(q.From().Joins()) != 0 || len(q.GroupBy()) != 0 || q.Having() != nil || q.Offset() != 0 || q.StartFrom() != "" || q.StartAfter() != "" {
		return Query{}, dal.ErrNotSupported
	}
	base := q.From().Base()
	ref, ok := base.(dal.CollectionRef)
	if !ok || ref.Parent() != nil || ref.Schema() != "" || ref.Database() != "" || base.Alias() != "" {
		return Query{}, dal.ErrNotSupported
	}
	native := Query{Collection: base.Name(), Limit: q.Limit()}
	for _, c := range q.Columns() {
		f, ok := c.Expression.(dal.FieldRef)
		if !ok || f.Source() != "" || (c.Alias != "" && c.Alias != f.Name()) {
			return Query{}, dal.ErrNotSupported
		}
		native.Projection = append(native.Projection, f.Name())
	}
	budget := 0
	var walk func(dal.Condition, int) error
	walk = func(condition dal.Condition, depth int) error {
		budget++
		if depth > 8 || budget > 64 {
			return dal.ErrNotSupported
		}
		switch c := condition.(type) {
		case nil:
			return nil
		case dal.GroupCondition:
			if c.Operator() != dal.And || len(c.Conditions()) == 0 {
				return dal.ErrNotSupported
			}
			for _, child := range c.Conditions() {
				if child == nil {
					return dal.ErrNotSupported
				}
				if e := walk(child, depth+1); e != nil {
					return e
				}
			}
		case dal.Comparison:
			field, ok := c.Left.(dal.FieldRef)
			if !ok || field.Source() != "" {
				return dal.ErrNotSupported
			}
			value, ok := c.Right.(dal.Constant)
			if !ok {
				return dal.ErrNotSupported
			}
			operator := ""
			switch c.Operator {
			case dal.Equal:
				operator = "=="
			case dal.GreaterThen:
				operator = ">"
			case dal.GreaterOrEqual:
				operator = ">="
			case dal.LessThen:
				operator = "<"
			case dal.LessOrEqual:
				operator = "<="
			default:
				return dal.ErrNotSupported
			}
			native.Filters = append(native.Filters, Filter{field.Name(), operator, value.Value})
		default:
			return dal.ErrNotSupported
		}
		return nil
	}
	if err := walk(q.Where(), 0); err != nil {
		return Query{}, err
	}
	for _, o := range q.OrderBy() {
		f, ok := o.Expression().(dal.FieldRef)
		if !ok || f.Source() != "" {
			return Query{}, dal.ErrNotSupported
		}
		native.Order = append(native.Order, Order{f.Name(), o.Descending()})
	}
	return native, nil
}

type recordsReader struct {
	ctx        context.Context
	cancel     context.CancelFunc
	client     *Client
	collection string
	rows       []Row
	index      int
	closed     bool
}

func (b *backend) ExecuteQueryToRecordsReader(ctx context.Context, q dal.Query) (dal.RecordsReader, error) {
	readCtx, cancel := context.WithTimeout(ctx, b.c.opts.Timeout)
	native, err := mappedQuery(q)
	if err != nil {
		cancel()
		return nil, err
	}
	unbounded := native.Limit == 0
	if unbounded {
		native.Limit = b.c.opts.MaxRows
	}
	page, err := b.c.Query(readCtx, native)
	if err != nil {
		cancel()
		return nil, err
	}
	if unbounded && len(page.NextCursor) > 0 {
		cancel()
		return nil, ErrBudget
	}
	return &recordsReader{ctx: readCtx, cancel: cancel, client: b.c, collection: native.Collection, rows: page.Rows}, nil
}
func (r *recordsReader) Close() error            { r.closed = true; r.cancel(); r.rows = nil; return nil }
func (r *recordsReader) Cursor() (string, error) { return "", dal.ErrNotSupported }
func (r *recordsReader) Next() (record.Record, error) {
	if r.closed {
		return nil, ErrClosed
	}
	if err := fresh(r.ctx); err != nil {
		return nil, err
	}
	if r.client.base.Err() != nil {
		return nil, ErrClosed
	}
	if r.index == len(r.rows) {
		return nil, dal.ErrNoMoreRecords
	}
	row := r.rows[r.index]
	data := make(map[string]any, len(row.Data))
	for k, v := range row.Data {
		data[k] = v
	}
	result := record.NewRecordWithData(record.NewKeyWithID(r.collection, row.Key), &data)
	result.SetError(nil)
	if err := fresh(r.ctx); err != nil {
		return nil, err
	}
	// Commit the delivered row only after the last freshness check. A late
	// cancellation must not roll back already acknowledged reader progress.
	r.index++
	return result, nil
}

var _ dal.RecordsReader = (*recordsReader)(nil)
