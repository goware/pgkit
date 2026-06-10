package pgkit

import (
	"fmt"
	"maps"
	"reflect"
	"slices"

	sq "github.com/Masterminds/squirrel"
)

type StatementBuilder struct {
	sq.StatementBuilderType
}

func (s *StatementBuilder) InsertRecord(record interface{}, optTableName ...string) InsertBuilder {
	tableName := getTableName(record, optTableName...)
	insert := sq.InsertBuilder(s.StatementBuilderType)

	cols, vals, err := Map(record)
	if err != nil {
		return InsertBuilder{InsertBuilder: insert, err: wrapErr(err)}
	}
	if len(cols) == 0 {
		hint := `SQL.InsertDefaults("<table>")`
		if tableName != "" {
			hint = fmt.Sprintf("SQL.InsertDefaults(%q)", tableName)
		}
		return InsertBuilder{InsertBuilder: insert, err: wrapErr(fmt.Errorf("Map returned no columns for %T; for an all-default INSERT use %s", record, hint))}
	}

	return InsertBuilder{InsertBuilder: insert.Into(tableName).Columns(cols...).Values(vals...)}
}

// InsertRecords builds a multi-row INSERT from a slice of records, unioning
// columns across rows and emitting DEFAULT for any slot a given row skipped.
func (s StatementBuilder) InsertRecords(recordsSlice interface{}, optTableName ...string) InsertBuilder {
	insert := sq.InsertBuilder(s.StatementBuilderType)

	v := reflect.ValueOf(recordsSlice)
	if v.Kind() != reflect.Slice {
		return InsertBuilder{InsertBuilder: insert, err: wrapErr(fmt.Errorf("records must be a slice type"))}
	}
	if v.Len() == 0 {
		return InsertBuilder{InsertBuilder: insert, err: wrapErr(fmt.Errorf("records slice is empty"))}
	}

	tableName := ""
	if len(optTableName) > 0 {
		tableName = optTableName[0]
	}

	rows := make([]map[string]any, 0, v.Len())
	colSet := map[string]struct{}{}
	for i := 0; i < v.Len(); i++ {
		record := v.Index(i).Interface()

		if i == 0 && tableName == "" {
			if getTableName, ok := record.(hasDBTableName); ok {
				tableName = getTableName.DBTableName()
			}
		}

		cols, vals, err := Map(record)
		if err != nil {
			return InsertBuilder{InsertBuilder: insert, err: wrapErr(err)}
		}
		byCol := make(map[string]any, len(cols))
		for j, c := range cols {
			byCol[c] = vals[j]
			colSet[c] = struct{}{}
		}
		rows = append(rows, byCol)
	}

	// slices.Sorted matches Map's lexical column order, so generated SQL
	// lines up with what callers see when they call Map(record) directly.
	allCols := slices.Sorted(maps.Keys(colSet))
	if len(allCols) == 0 {
		hint := `SQL.InsertDefaults("<table>")`
		if tableName != "" {
			hint = fmt.Sprintf("SQL.InsertDefaults(%q)", tableName)
		}
		return InsertBuilder{InsertBuilder: insert, err: wrapErr(fmt.Errorf("Map returned no columns across any of the %d records; for all-default rows use %s in a loop", v.Len(), hint))}
	}

	insert = insert.Into(tableName).Columns(allCols...)
	for _, row := range rows {
		padded := make([]any, len(allCols))
		for i, c := range allCols {
			if v, ok := row[c]; ok {
				padded[i] = v
			} else {
				padded[i] = sqlDefault
			}
		}
		insert = insert.Values(padded...)
	}

	return InsertBuilder{InsertBuilder: insert}
}

// InsertDefaults builds INSERT INTO <table> DEFAULT VALUES; table must be non-empty.
func (s StatementBuilder) InsertDefaults(table string) DefaultValuesBuilder {
	if table == "" {
		// raw error; Querier.wrapErr applies the pgkit: prefix at use time.
		return DefaultValuesBuilder{err: fmt.Errorf("insert statements must specify a table")}
	}
	return DefaultValuesBuilder{table: table}
}

// DefaultValuesBuilder is the sq.Sqlizer returned by InsertDefaults.
type DefaultValuesBuilder struct {
	table  string
	suffix string
	err    error
}

func (b DefaultValuesBuilder) ToSql() (string, []any, error) {
	if b.err != nil {
		return "", nil, b.err
	}
	// Defensive: a zero-value DefaultValuesBuilder bypasses the InsertDefaults
	// constructor's table check, so re-validate here.
	if b.table == "" {
		return "", nil, fmt.Errorf("insert statements must specify a table")
	}
	return "INSERT INTO " + b.table + " DEFAULT VALUES" + b.suffix, nil, nil
}

// Suffix appends literal SQL; no placeholder rewriting, use sq.Expr for that.
func (b DefaultValuesBuilder) Suffix(sql string) DefaultValuesBuilder {
	if b.err != nil || sql == "" {
		return b
	}
	return DefaultValuesBuilder{table: b.table, suffix: b.suffix + " " + sql}
}

func (b DefaultValuesBuilder) Err() error { return b.err }

func (s StatementBuilder) UpdateRecord(record interface{}, whereExpr sq.Eq, optTableName ...string) UpdateBuilder {
	return s.UpdateRecordColumns(record, whereExpr, nil, optTableName...)
}

func (s StatementBuilder) UpdateRecordColumns(record interface{}, whereExpr sq.Eq, filterCols []string, optTableName ...string) UpdateBuilder {
	tableName := getTableName(record, optTableName...)
	update := sq.UpdateBuilder(s.StatementBuilderType)

	cols, vals, err := Map(record)
	if err != nil {
		return UpdateBuilder{UpdateBuilder: update, err: wrapErr(err)}
	}

	// when filter is empty or nil, update the entire record
	var filter []string
	if len(filterCols) != 0 {
		filter = filterCols
	}

	valMap, err := createMap(cols, vals, filter)
	if err != nil {
		return UpdateBuilder{UpdateBuilder: update, err: wrapErr(err)}
	}

	return UpdateBuilder{UpdateBuilder: update.Table(tableName).SetMap(valMap).Where(whereExpr)}
}

type InsertBuilder struct {
	sq.InsertBuilder
	err error
}

func (b InsertBuilder) Err() error { return b.err }

type UpdateBuilder struct {
	sq.UpdateBuilder
	err error
}

func (b UpdateBuilder) Err() error { return b.err }

func getTableName(record interface{}, optTableName ...string) string {
	tableName := ""
	if len(optTableName) > 0 {
		tableName = optTableName[0]
	} else {
		if getTableName, ok := record.(hasDBTableName); ok {
			tableName = getTableName.DBTableName()
		}
	}
	return tableName
}

func createMap(k []string, v []interface{}, filterK []string) (map[string]interface{}, error) {
	if len(k) != len(v) {
		return nil, fmt.Errorf("key and value pair is not of equal length")
	}

	m := make(map[string]interface{}, len(k))

	for i := 0; i < len(k); i++ {
		if len(filterK) == 0 {
			m[k[i]] = v[i]
			continue
		}
		for x := 0; x < len(filterK); x++ {
			if filterK[x] == k[i] {
				m[k[i]] = v[i]
				break
			}
		}
	}

	return m, nil
}
