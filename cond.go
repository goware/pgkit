package pgkit

import (
	"fmt"
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

type binaryExprLeaf struct {
	op string

	v interface{}
}

func (c binaryExprLeaf) ToSql() (string, []interface{}, error) {
	if c.v == nil {
		return "", nil, nil
	}

	return "?", []interface{}{c.v}, nil
}

type binaryExprNode struct {
	left  interface{}
	right interface{}
}

func (n *binaryExprNode) ToSql() (string, []interface{}, error) {
	_, leftIsString := n.left.(string)
	_, rightIsExpr := n.right.(squirrel.Sqlizer)

	if leftIsString && !rightIsExpr {
		// backwards compatibility with old-style conditions
		return squirrel.Eq{n.left.(string): n.right}.ToSql()
	}

	ql, argsl, err := compileLeaf(n.left)
	if err != nil {
		return "", nil, fmt.Errorf("error compiling left side: %w", err)
	}

	rl, argsr, err := compileLeaf(n.right)
	if err != nil {
		return "", nil, fmt.Errorf("error compiling right side: %w", err)
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
			return "", nil, fmt.Errorf("error compiling node %d: %w", i, err)
		}

		q += qn
		args = append(args, argsn...)
	}

	return q, args, nil
}

func compileOperator(leaf interface{}) string {
	if expr, ok := leaf.(*binaryExprLeaf); ok {
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
		nodes = append(nodes, &binaryExprNode{left, right})
	}

	return compileNodes(nodes)
}

// Eq represents an equality comparison.
func Eq(v interface{}) squirrel.Sqlizer {
	return &binaryExprLeaf{"=", v}
}

// NotEq represents a not-equal comparison.
func NotEq(v interface{}) squirrel.Sqlizer {
	return &binaryExprLeaf{"<>", v}
}

// Gt represents a greater-than comparison.
func Gt(v interface{}) squirrel.Sqlizer {
	return &binaryExprLeaf{">", v}
}

// Gte represents a greater-than-or-equal comparison.
func Gte(v interface{}) squirrel.Sqlizer {
	return &binaryExprLeaf{">=", v}
}

// Lt represents a less-than comparison.
func Lt(v interface{}) squirrel.Sqlizer {
	return &binaryExprLeaf{"<", v}
}

// Lte represents a less-than-or-equal comparison.
func Lte(v interface{}) squirrel.Sqlizer {
	return &binaryExprLeaf{"<=", v}
}

// IsNull represents an IS NULL comparison.
func IsNull() squirrel.Sqlizer {
	return &binaryExprLeaf{"IS NULL", nil}
}

// IsNotNull represents an IS NOT NULL comparison.
func IsNotNull() squirrel.Sqlizer {
	return &binaryExprLeaf{"IS NOT NULL", nil}
}

// Like represents a LIKE comparison.
func Like(v interface{}) squirrel.Sqlizer {
	return &binaryExprLeaf{"LIKE", v}
}

// NotLike represents a NOT LIKE comparison.
func NotLike(v interface{}) squirrel.Sqlizer {
	return &binaryExprLeaf{"NOT LIKE", v}
}

// ILike represents a ILIKE comparison.
func ILike(v interface{}) squirrel.Sqlizer {
	return &binaryExprLeaf{"ILIKE", v}
}

// NotILike represents a NOT ILIKE comparison.
func NotILike(v interface{}) squirrel.Sqlizer {
	return &binaryExprLeaf{"NOT ILIKE", v}
}

// In represents an IN operator. The value must be variadic.
func In[T interface{}](v ...T) squirrel.Sqlizer {
	return sqlExprFn(func() (string, []interface{}, error) {
		if len(v) == 0 {
			return "IN ()", nil, nil
		}

		args := make([]interface{}, len(v))
		for i, val := range v {
			args[i] = val
		}

		return "IN (?" + strings.Repeat(", ?", len(v)-1) + ")", args, nil
	})
}

// NotIn represents a NOT IN operator. The value must be variadic.
func NotIn[T interface{}](v ...T) squirrel.Sqlizer {
	return sqlExprFn(func() (string, []interface{}, error) {
		if len(v) == 0 {
			return "NOT IN ()", nil, nil
		}

		args := make([]interface{}, len(v))
		for i, val := range v {
			args[i] = val
		}

		return "NOT IN (?" + strings.Repeat(", ?", len(v)-1) + ")", args, nil
	})
}

func Raw(sql string, args ...interface{}) squirrel.Sqlizer {
	return sqlExprFn(func() (string, []interface{}, error) {
		return sql, args, nil
	})
}
