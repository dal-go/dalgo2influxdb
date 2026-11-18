package dalgo2influxdb

import (
	"encoding/json"
	"math"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

var jsonNumber = regexp.MustCompile("^-(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$|^(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$")

type plan struct {
	query    Query
	table    Table
	sql      string
	params   map[string]any
	selected []string
	output   []string
	order    []Order
	limit    int
}

func validName(s string) bool {
	if s == "" || len(s) > 128 || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func quote(s string) string { return "\"" + strings.ReplaceAll(s, "\"", "\"\"") + "\"" }

func scalar(v any) (any, string, bool) {
	switch x := v.(type) {
	case nil:
		return nil, "null", true
	case string:
		if !utf8.ValidString(x) {
			return nil, "", false
		}
		return x, "string", true
	case bool:
		return x, "boolean", true
	case json.Number:
		if len(x) > 128 || !jsonNumber.MatchString(string(x)) {
			return nil, "", false
		}
		if i := strings.IndexAny(string(x), "eE"); i >= 0 {
			exponent, e := strconv.ParseInt(string(x)[i+1:], 10, 32)
			if e != nil || exponent < -4096 || exponent > 4096 {
				return nil, "", false
			}
		}
		return x, "number", true
	case int:
		return json.Number(strconv.FormatInt(int64(x), 10)), "number", true
	case int8:
		return scalar(int64(x))
	case int16:
		return scalar(int64(x))
	case int32:
		return scalar(int64(x))
	case int64:
		return json.Number(strconv.FormatInt(x, 10)), "number", true
	case uint:
		return scalar(uint64(x))
	case uint8:
		return scalar(uint64(x))
	case uint16:
		return scalar(uint64(x))
	case uint32:
		return scalar(uint64(x))
	case uint64:
		return json.Number(strconv.FormatUint(x, 10)), "number", true
	case float32:
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return nil, "", false
		}
		return json.Number(strconv.FormatFloat(float64(x), 'g', -1, 32)), "number", true
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, "", false
		}
		return json.Number(strconv.FormatFloat(x, 'g', -1, 64)), "number", true
	default:
		return nil, "", false
	}
}

func column(t Table, field string) (Column, bool) {
	if field == "__name__" {
		return t.Key, true
	}
	c, ok := t.Columns[field]
	return c, ok
}

func compile(tables map[string]Table, q Query, maxRows, maxRequestBytes int) (plan, error) {
	t, ok := tables[q.Collection]
	if !ok || len(q.Filters) > 64 || len(q.Order) > 16 || len(q.Projection) > 128 {
		return plan{}, ErrQuery
	}
	limit := q.Limit
	if limit == 0 {
		limit = min(100, maxRows)
	}
	if limit < 1 || limit > maxRows {
		return plan{}, ErrQuery
	}
	for _, f := range q.Filters {
		if s, ok := f.Value.(string); ok && len(s) > maxRequestBytes {
			return plan{}, ErrBudget
		}
	}
	for _, v := range q.StartAfter {
		if s, ok := v.(string); ok && len(s) > maxRequestBytes {
			return plan{}, ErrBudget
		}
	}
	q.Projection = slices.Clone(q.Projection)
	q.Filters = slices.Clone(q.Filters)
	q.Order = slices.Clone(q.Order)
	q.StartAfter = slices.Clone(q.StartAfter)
	p := plan{query: q, table: t, params: map[string]any{}, limit: limit}
	if len(q.Projection) == 0 {
		for field := range t.Columns {
			p.output = append(p.output, field)
		}
		sort.Strings(p.output)
	} else {
		seen := map[string]bool{}
		for _, field := range q.Projection {
			if field == "__name__" || seen[field] {
				return plan{}, ErrQuery
			}
			if _, ok := t.Columns[field]; !ok {
				return plan{}, ErrQuery
			}
			seen[field] = true
			p.output = append(p.output, field)
		}
	}
	p.selected = slices.Clone(p.output)
	for _, f := range q.Filters {
		if f.Field != "__name__" {
			if _, ok := t.Columns[f.Field]; !ok {
				return plan{}, ErrQuery
			}
			if !slices.Contains(p.selected, f.Field) {
				p.selected = append(p.selected, f.Field)
			}
		}
	}
	seenOrder := map[string]bool{}
	for i, order := range q.Order {
		c, ok := column(t, order.Field)
		if !ok || seenOrder[order.Field] || c.Nullable || (order.Field == "__name__" && i != len(q.Order)-1) {
			return plan{}, ErrQuery
		}
		seenOrder[order.Field] = true
		p.order = append(p.order, order)
		if order.Field != "__name__" && !slices.Contains(p.selected, order.Field) {
			p.selected = append(p.selected, order.Field)
		}
	}
	if !seenOrder["__name__"] {
		p.order = append(p.order, Order{Field: "__name__"})
	}
	selects := []string{quote(t.Key.Name) + " AS " + quote("__dalgo_key")}
	for _, f := range p.selected {
		selects = append(selects, quote(t.Columns[f].Name)+" AS "+quote(f))
	}
	var where []string
	ops := map[string]string{"==": "=", "!=": "<>", "<": "<", "<=": "<=", ">": ">", ">=": ">="}
	bindingBytes := 0
	overflow := false
	bind := func(v any) string {
		if overflow {
			return ""
		}
		name := "p" + strconv.Itoa(len(p.params))
		encoded, e := json.Marshal(v)
		if e != nil || len(encoded) > maxRequestBytes-bindingBytes-len(name)-4 {
			overflow = true
			return ""
		}
		bindingBytes += len(encoded) + len(name) + 4
		p.params[name] = v
		return "$" + name
	}
	for _, f := range q.Filters {
		c, found := column(t, f.Field)
		op, allowed := ops[f.Operator]
		v, kind, valid := scalar(f.Value)
		if !found || !allowed || !valid || (kind != "null" && c.Kind != "" && c.Kind != kind) {
			return plan{}, ErrQuery
		}
		if kind == "null" {
			if f.Operator == "==" {
				where = append(where, quote(c.Name)+" IS NULL")
			} else if f.Operator == "!=" {
				where = append(where, quote(c.Name)+" IS NOT NULL")
			} else {
				return plan{}, ErrQuery
			}
		} else {
			where = append(where, quote(c.Name)+" "+op+" "+bind(v))
		}
	}
	if len(q.StartAfter) > 0 {
		if len(q.StartAfter) != len(p.order) {
			return plan{}, ErrQuery
		}
		values := make([]any, len(q.StartAfter))
		for i, v := range q.StartAfter {
			c, _ := column(t, p.order[i].Field)
			s, kind, valid := scalar(v)
			if !valid || kind == "null" || (c.Kind != "" && c.Kind != kind) || (p.order[i].Field == "__name__" && kind != "string") {
				return plan{}, ErrQuery
			}
			values[i] = s
		}
		var disjunction []string
		for i, o := range p.order {
			var terms []string
			for j := 0; j < i; j++ {
				c, _ := column(t, p.order[j].Field)
				terms = append(terms, quote(c.Name)+" = "+bind(values[j]))
			}
			c, _ := column(t, o.Field)
			op := ">"
			if o.Descending {
				op = "<"
			}
			terms = append(terms, quote(c.Name)+" "+op+" "+bind(values[i]))
			disjunction = append(disjunction, "("+strings.Join(terms, " AND ")+")")
		}
		where = append(where, "("+strings.Join(disjunction, " OR ")+")")
	}
	p.sql = "SELECT " + strings.Join(selects, ", ") + " FROM " + quote(t.Name)
	if len(where) > 0 {
		p.sql += " WHERE " + strings.Join(where, " AND ")
	}
	var orders []string
	for _, o := range p.order {
		c, _ := column(t, o.Field)
		direction := " ASC"
		if o.Descending {
			direction = " DESC"
		}
		orders = append(orders, quote(c.Name)+direction)
	}
	p.sql += " ORDER BY " + strings.Join(orders, ", ") + " LIMIT " + strconv.Itoa(limit+1)
	if overflow {
		return plan{}, ErrBudget
	}
	return p, nil
}
