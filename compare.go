package dalgo2influxdb

import (
	"encoding/json"
	"math/big"
	"strings"
)

func compareScalar(a, b any) (int, bool) {
	a, ka, ok := scalar(a)
	if !ok {
		return 0, false
	}
	b, kb, ok := scalar(b)
	if !ok || ka != kb {
		return 0, false
	}
	switch ka {
	case "null":
		return 0, true
	case "string":
		return strings.Compare(a.(string), b.(string)), true
	case "boolean":
		if a == b {
			return 0, true
		}
		if a == false {
			return -1, true
		}
		return 1, true
	case "number":
		x, ok := new(big.Rat).SetString(string(a.(json.Number)))
		if !ok {
			return 0, false
		}
		y, ok := new(big.Rat).SetString(string(b.(json.Number)))
		if !ok {
			return 0, false
		}
		return x.Cmp(y), true
	default:
		return 0, false
	}
}

func compareRows(a, b map[string]any, order []Order) int {
	for _, o := range order {
		field := o.Field
		if field == "__name__" {
			field = "__dalgo_key"
		}
		c, ok := compareScalar(a[field], b[field])
		if !ok {
			return 0
		}
		if o.Descending {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

func matches(value any, f Filter) bool {
	if f.Value == nil {
		if f.Operator == "==" {
			return value == nil
		}
		if f.Operator == "!=" {
			return value != nil
		}
		return false
	}
	if value == nil {
		return false
	}
	c, ok := compareScalar(value, f.Value)
	if !ok {
		return false
	}
	switch f.Operator {
	case "==":
		return c == 0
	case "!=":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	}
	return false
}
