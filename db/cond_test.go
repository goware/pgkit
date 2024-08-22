package db_test

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goware/pgkit/v2/db"
)

func TestCond(t *testing.T) {
	t.Run("equal to", func(t *testing.T) {
		cond := db.Cond{"one": 1}
		s, args, err := cond.ToSql()
		require.NoError(t, err)
		assert.Equal(t, []interface{}{1}, args)
		assert.Equal(t, "one = ?", s)
	})

	t.Run("equal to with multiple parameters", func(t *testing.T) {
		cond := db.And{db.Cond{"one": 1}, db.Cond{"two": 2}}
		s, args, err := cond.ToSql()
		require.NoError(t, err)
		assert.Equal(t, []interface{}{1, 2}, args)
		assert.Equal(t, "(one = ? AND two = ?)", s)
	})

	t.Run("equal to (inverted)", func(t *testing.T) {
		cond := db.Cond{1: "one"}
		s, args, err := cond.ToSql()
		require.NoError(t, err)
		assert.Equal(t, []interface{}{1}, args)
		assert.Equal(t, "? = one", s)
	})

	t.Run("equal to subquery", func(t *testing.T) {
		q := sq.Select("id").From("users").Where(db.Cond{"badge": "admin"})

		cond := db.Cond{"id": q}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Equal(t, "id = (SELECT id FROM users WHERE badge = ?)", s)
		assert.Equal(t, []interface{}{"admin"}, args)
	})

	t.Run("less than or equal", func(t *testing.T) {
		cond := db.Cond{"id": db.Lte(1)}
		s, args, err := cond.ToSql()
		require.NoError(t, err)
		assert.Equal(t, []interface{}{1}, args)
		assert.Equal(t, "id <= ?", s)
	})

	t.Run("single node", func(t *testing.T) {
		cond := db.Lte(1)
		s, args, err := cond.ToSql()
		require.NoError(t, err)
		assert.Equal(t, []interface{}{1}, args)
		assert.Equal(t, "?", s)
	})

	t.Run("IS NULL", func(t *testing.T) {
		cond := db.Cond{"status": db.IsNull()}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Empty(t, args)
		assert.Equal(t, "status IS NULL", s)
	})

	t.Run("IN with slice", func(t *testing.T) {
		sl1 := []int{1, 2, 3}
		cond := db.Cond{"list": db.In(sl1...)}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Equal(t, []interface{}{1, 2, 3}, args)
		assert.Equal(t, "list IN (?, ?, ?)", s)
	})

	t.Run("IN with slice variadic", func(t *testing.T) {
		cond := db.Cond{"list": db.In(1, 2, 3)}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Equal(t, []interface{}{1, 2, 3}, args)
		assert.Equal(t, "list IN (?, ?, ?)", s)
	})

	t.Run("multiple IN with slice", func(t *testing.T) {
		sl1 := []int{1, 2, 3}
		sl2 := []int{4, 5, 6}
		cond := db.Cond{"list": db.In([]interface{}{sl1, sl2}...)}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Equal(t, []interface{}{1, 2, 3, 4, 5, 6}, args)
		assert.Equal(t, "list IN ((?,?,?),(?,?,?))", s)
	})

	t.Run("multiple IN with slice AND where ID", func(t *testing.T) {
		cond := db.And{db.Cond{"list": db.In([][]string{{"1", "2", "3"}, {"3", "4", "5"}}...)}, db.Cond{"id": 1}}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Equal(t, []interface{}{"1", "2", "3", "3", "4", "5", 1}, args)
		assert.Equal(t, "(list IN ((?,?,?),(?,?,?)) AND id = ?)", s)
	})

	t.Run("multiple IN with struct", func(t *testing.T) {
		randomStruct := []struct {
			Id   uint64
			Name string
		}{
			{Id: 1, Name: "Lukas"},
			{Id: 2, Name: "David"},
		}

		data := [][]interface{}{}
		for _, s := range randomStruct {
			data = append(data, []interface{}{s.Id, s.Name})
		}

		cond := db.Cond{"list": db.In(data...)}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Equal(t, []interface{}{uint64(1), "Lukas", uint64(2), "David"}, args)
		assert.Equal(t, "list IN ((?,?),(?,?))", s)
	})

	t.Run("NOT IN", func(t *testing.T) {
		cond := db.Cond{"list": db.NotIn("Czech Republic", "Slovakia")}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Equal(t, []interface{}{"Czech Republic", "Slovakia"}, args)
		assert.Equal(t, "list NOT IN (?, ?)", s)
	})

	t.Run("IN with empty slice", func(t *testing.T) {
		cond := db.Cond{"list": db.In[any]()}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Empty(t, args)
		assert.Equal(t, "list IN ()", s)
	})

	t.Run("NOT IN with empty slice", func(t *testing.T) {
		cond := db.Cond{"list": db.NotIn[any]()}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Empty(t, args)
		assert.Equal(t, "list NOT IN ()", s)
	})

	t.Run("IN with slice of strings", func(t *testing.T) {
		{
			cond := db.Cond{"list": db.In[string]("Czech Republic", "Slovakia")}
			s, args, err := cond.ToSql()
			require.NoError(t, err)

			assert.Equal(t, []interface{}{"Czech Republic", "Slovakia"}, args)
			assert.Equal(t, "list IN (?, ?)", s)
		}

		{
			cond := db.Cond{"list": db.In[interface{}]("Czech Republic", "Slovakia")}
			s, args, err := cond.ToSql()
			require.NoError(t, err)

			assert.Equal(t, []interface{}{"Czech Republic", "Slovakia"}, args)
			assert.Equal(t, "list IN (?, ?)", s)
		}

		{
			list := []string{"Czech Republic", "Slovakia"}
			cond := db.Cond{"list": db.NotIn[string](list...)}
			s, args, err := cond.ToSql()
			require.NoError(t, err)

			assert.Equal(t, []interface{}{"Czech Republic", "Slovakia"}, args)
			assert.Equal(t, "list NOT IN (?, ?)", s)
		}
	})

	t.Run("raw condition", func(t *testing.T) {
		cond := db.Cond{"salary": db.Raw("> ANY(SELECT salary FROM managers WHERE id < ?)", 23)}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Equal(t, []interface{}{23}, args)
		assert.Equal(t, "salary > ANY(SELECT salary FROM managers WHERE id < ?)", s)
	})

	t.Run("LIKE with string value", func(t *testing.T) {
		cond := db.Cond{"name": db.Like("john")}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Equal(t, []interface{}{"john"}, args)
		assert.Equal(t, "name LIKE ?", s)
	})

	t.Run("ANY(list) = 1", func(t *testing.T) {
		{
			cond := db.Cond{db.Raw("ANY(list)"): 1}
			s, args, err := cond.ToSql()
			require.NoError(t, err)

			assert.Equal(t, []interface{}{1}, args)
			assert.Equal(t, "ANY(list) = ?", s)
		}

		{
			cond := db.Cond{db.Func("ANY", db.Raw("list")): 1}
			s, args, err := cond.ToSql()
			require.NoError(t, err)

			assert.Equal(t, []interface{}{1}, args)
			assert.Equal(t, "ANY (list) = ?", s)
		}
	})

	t.Run("ANY(list) <> 1", func(t *testing.T) {
		cond := db.Cond{db.Raw("ANY(list)"): db.NotEq(1)}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Equal(t, []interface{}{1}, args)
		assert.Equal(t, "ANY(list) <> ?", s)
	})

	t.Run("id IN subquery", func(t *testing.T) {
		q := sq.Select("id").From("users").Where(
			db.Or{
				db.Cond{"status": db.Eq("active")},
				db.Cond{"status": db.Eq("banned")},
			},
		)

		cond := db.Cond{"id": db.In(q)}
		s, args, err := cond.ToSql()
		require.NoError(t, err)

		assert.Equal(t, []interface{}{"active", "banned"}, args)
		assert.Equal(t, "id IN (SELECT id FROM users WHERE (status = ? OR status = ?))", s)
	})
}
