package pgkit

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/Masterminds/squirrel"
)

type sqlExpr struct {
	fn func() (string, []interface{}, error)
}

func (s sqlExpr) ToSql() (string, []interface{}, error) {
	return s.fn()
}

func sqlExprFn(fn func() (string, []interface{}, error)) *sqlExpr {
	return &sqlExpr{fn}
}

type condExpr struct {
	op string

	v interface{}
}

func (c condExpr) ToSql() (string, []interface{}, error) {
	if c.v == nil {
		return "", nil, nil
	}
	return "?", []interface{}{c.v}, nil
}

type condNode struct {
	left  interface{}
	right interface{}
}

func (n *condNode) ToSql() (string, []interface{}, error) {
	_, leftIsString := n.left.(string)
	_, rightIsExpr := n.right.(squirrel.Sqlizer)

	if leftIsString && !rightIsExpr {
		// backwards compatibility with old-style conditions
		return squirrel.Eq{n.left.(string): n.right}.ToSql()
	}

	ql, argsl, err := compileLeaf(n.left)
	if err != nil {
		return "", nil, fmt.Errorf("error compiling left side: %v", err)
	}

	rl, argsr, err := compileLeaf(n.right)
	if err != nil {
		return "", nil, fmt.Errorf("error compiling right side: %v", err)
	}

	// always take operator from right side
	if rl == "" {
		return ql + compileOperator(n.right), argsl, nil
	}

	return string(ql) + compileOperator(n.right) + " " + string(rl), append(argsl, argsr...), nil
}

func compileNodes(nodes []squirrel.Sqlizer) (q string, args []interface{}, err error) {
	for i, node := range nodes {
		qn, argsn, err := node.ToSql()
		if err != nil {
			return "", nil, fmt.Errorf("error compiling node %d: %v", i, err)
		}
		q += qn
		args = append(args, argsn...)
	}

	return q, args, nil
}

func compileOperator(leaf interface{}) string {
	if expr, ok := leaf.(*condExpr); ok {
		return " " + expr.op
	}
	if _, ok := leaf.(squirrel.Sqlizer); ok {
		return ""
	}
	return " ="
}

func compileLeaf(leaf interface{}) (string, []interface{}, error) {
	if _, ok := leaf.(string); ok {
		return leaf.(string), nil, nil
	}

	if sqlizer, ok := leaf.(squirrel.Sqlizer); ok {
		return sqlizer.ToSql()
	}

	return "?", []interface{}{leaf}, nil
}

// Cond is a map of conditions.
type Cond map[interface{}]interface{}

func (c Cond) ToSql() (string, []interface{}, error) {
	nodes := []squirrel.Sqlizer{}
	for left, right := range c {
		nodes = append(nodes, &condNode{left, right})
	}
	return compileNodes(nodes)
}

// Eq represents an equality comparison.
func Eq(v interface{}) squirrel.Sqlizer {
	return &condExpr{"=", v}
}

// Lt represents a less-than comparison.
func Lt(v interface{}) squirrel.Sqlizer {
	return &condExpr{"<", v}
}

// Lte represents a less-than-or-equal comparison.
func Lte(v interface{}) squirrel.Sqlizer {
	return &condExpr{"<=", v}
}

// IsNull represents an IS NULL comparison.
func IsNull() squirrel.Sqlizer {
	return &condExpr{"IS NULL", nil}
}

// IsNotNull represents an IS NOT NULL comparison.
func IsNotNull() squirrel.Sqlizer {
	return &condExpr{"IS NOT NULL", nil}
}

// Gt represents a greater-than comparison.
func Gt(v interface{}) squirrel.Sqlizer {
	return &condExpr{">", v}
}

// Gte represents a greater-than-or-equal comparison.
func Gte(v interface{}) squirrel.Sqlizer {
	return &condExpr{">=", v}
}

// NotEq represents a not-equal comparison.
func NotEq(v interface{}) squirrel.Sqlizer {
	return &condExpr{"<>", v}
}

// Like represents a LIKE comparison.
func Like(v interface{}) squirrel.Sqlizer {
	return &condExpr{"LIKE", v}
}

// ILike represents a ILIKE comparison.
func ILike(v interface{}) squirrel.Sqlizer {
	return &condExpr{"ILIKE", v}
}

// NotILike represents a NOT ILIKE comparison.
func NotILike(v interface{}) squirrel.Sqlizer {
	return &condExpr{"NOT ILIKE", v}
}

// NotLike represents a NOT LIKE comparison.
func NotLike(v interface{}) squirrel.Sqlizer {
	return &condExpr{"NOT LIKE", v}
}

// In represents an IN operator. The value must be variadic.
func In(v ...interface{}) squirrel.Sqlizer {
	return sqlExprFn(func() (string, []interface{}, error) {
		if len(v) == 0 {
			return "IN ()", nil, nil
		}
		return "IN (?" + strings.Repeat(", ?", len(v)-1) + ")", v, nil
	})
}

// NotIn represents a NOT IN operator. The value must be variadic.
func NotIn(v ...interface{}) squirrel.Sqlizer {
	return sqlExprFn(func() (string, []interface{}, error) {
		if len(v) == 0 {
			return "NOT IN ()", nil, nil
		}
		return "NOT IN (?" + strings.Repeat(", ?", len(v)-1) + ")", v, nil
	})
}

// AnyOf represents an IN operator. The value must be a slice.
func AnyOf(v interface{}) squirrel.Sqlizer {
	return sqlExprFn(func() (string, []interface{}, error) {
		rv := reflect.ValueOf(v)

		if rv.Kind() != reflect.Slice {
			return "", nil, fmt.Errorf("value must be a slice")
		}
		if rv.Len() == 0 {
			return "IN ()", nil, nil
		}

		vs := make([]interface{}, rv.Len())

		for i := 0; i < rv.Len(); i++ {
			vs[i] = rv.Index(i).Interface()
		}

		return "IN (?" + strings.Repeat(", ?", len(vs)-1) + ")", vs, nil
	})
}

// NotAnyOf represents a NOT IN operator. The value must be a slice.
func NotAnyOf(v interface{}) squirrel.Sqlizer {
	return sqlExprFn(func() (string, []interface{}, error) {
		rv := reflect.ValueOf(v)
		if rv.Kind() != reflect.Slice {
			return "", nil, fmt.Errorf("value must be a slice")
		}

		if rv.Len() == 0 {
			return "NOT IN ()", nil, nil
		}

		vs := make([]interface{}, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			vs[i] = rv.Index(i).Interface()
		}

		return "NOT IN (?" + strings.Repeat(", ?", len(vs)-1) + ")", vs, nil
	})
}

func Raw(sql string, args ...interface{}) squirrel.Sqlizer {
	return sqlExprFn(func() (string, []interface{}, error) {
		return sql, args, nil
	})
}
