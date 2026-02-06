package pgkit

import (
	"fmt"
	"reflect"

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

	return InsertBuilder{InsertBuilder: insert.Into(tableName).Columns(cols...).Values(vals...)}
}

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

		if i == 0 {
			insert = insert.Columns(cols...).Values(vals...)
		} else {
			insert = insert.Values(vals...)
		}
	}

	return InsertBuilder{InsertBuilder: insert.Into(tableName)}
}

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
