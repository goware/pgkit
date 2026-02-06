package pgkit

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/goware/pgkit/v2/internal/reflectx"
)

const (
	dbTagName   = `db`
	dbTagPrefix = `db:"`
)

var Mapper = reflectx.NewMapper(dbTagName)

var (
	defaultMapOptions = MapOptions{
		IncludeZeroed: false,
		IncludeNil:    false,
	}

	sqlDefault = sq.Expr("DEFAULT")
	// sqlNULL    = sq.Expr("NULL")

	ErrExpectingPointerToEitherMapOrStruct = fmt.Errorf(`expecting a pointer to either a map or a struct`)
)

type MapOptions struct {
	IncludeZeroed bool
	IncludeNil    bool
}

// Map converts a struct object (aka record) to a mapping of column names and values
// which can be directly passed to a query executor. This allows you to use structs/objects
// to build easy insert/update queries without having to specify the column names manually.
// The mapper works by reading the column names from a struct fields `db:""` struct tag.
// If you specify `,omitempty` as a tag option, then it will omit the column from the list,
// which allows the database to take over and use its default value.
func Map(record interface{}) ([]string, []interface{}, error) {
	return MapWithOptions(record, nil)
}

func MapWithOptions(record interface{}, options *MapOptions) ([]string, []interface{}, error) {
	var fv fieldValue
	if options == nil {
		options = &defaultMapOptions
	}

	recordV := reflect.ValueOf(record)
	if !recordV.IsValid() {
		return nil, nil, nil
	}

	recordT := recordV.Type()

	if recordT.Kind() == reflect.Ptr {
		// Single dereference. Just in case the user passes a pointer to struct
		// instead of a struct.
		record = recordV.Elem().Interface()
		recordV = reflect.ValueOf(record)
		recordT = recordV.Type()
	}

	// TODO: for the same "type", we can cache the fieldinfo, etc. as it will be the same
	// on subsequent loads

	switch recordT.Kind() {

	case reflect.Struct:
		fieldMap := Mapper.TypeMap(recordT).Names
		nfields := len(fieldMap)

		fv.values = make([]interface{}, 0, nfields)
		fv.fields = make([]string, 0, nfields)

		for _, fi := range fieldMap {

			// Skip any fields which do not specify the `db:".."` tag
			if !strings.Contains(string(fi.Field.Tag), dbTagPrefix) {
				continue
			}

			// Field options
			_, tagOmitEmpty := fi.Options["omitempty"]

			fld := reflectx.FieldByIndexesReadOnly(recordV, fi.Index)

			if fld.Kind() == reflect.Ptr && fld.IsNil() {
				if tagOmitEmpty && !options.IncludeNil {
					continue
				}
				fv.fields = append(fv.fields, fi.Name)
				if tagOmitEmpty {
					fv.values = append(fv.values, sqlDefault)
				} else {
					fv.values = append(fv.values, nil)
				}
				continue
			}

			value := fld.Interface()

			isZero := false
			if t, ok := fld.Interface().(hasIsZero); ok {
				if t.IsZero() {
					isZero = true
				}
			} else if fld.Kind() == reflect.Array || fld.Kind() == reflect.Slice {
				if fld.Len() == 0 {
					isZero = true
				}
			} else if reflect.DeepEqual(fi.Zero.Interface(), value) {
				isZero = true
			}

			if isZero && tagOmitEmpty && !options.IncludeZeroed {
				continue
			}

			fv.fields = append(fv.fields, fi.Name)
			// v, err := marshal(value)
			// if err != nil {
			// 	return nil, nil, err
			// }
			v := value
			if isZero && tagOmitEmpty {
				v = sqlDefault
			}
			fv.values = append(fv.values, v)
		}

	case reflect.Map:
		nfields := recordV.Len()
		fv.values = make([]interface{}, nfields)
		fv.fields = make([]string, nfields)
		mkeys := recordV.MapKeys()

		for i, keyV := range mkeys {
			valv := recordV.MapIndex(keyV)
			fv.fields[i] = fmt.Sprintf("%v", keyV.Interface())

			// v, err := marshal(valv.Interface())
			// if err != nil {
			// 	return nil, nil, err
			// }
			v := valv

			fv.values[i] = v
		}

	default:
		return nil, nil, ErrExpectingPointerToEitherMapOrStruct
	}

	// sanity check -- we must have equal number of columns and values
	if len(fv.fields) != len(fv.values) {
		return fv.fields, fv.values, fmt.Errorf("record mapper returned %d columns and %d values", len(fv.fields), len(fv.values))
	}

	// normalize order for better cache hits
	sort.Sort(&fv)

	return fv.fields, fv.values, nil
}

type fieldValue struct {
	fields []string
	values []interface{}
}

func (fv *fieldValue) Len() int {
	return len(fv.fields)
}

func (fv *fieldValue) Swap(i, j int) {
	fv.fields[i], fv.fields[j] = fv.fields[j], fv.fields[i]
	fv.values[i], fv.values[j] = fv.values[j], fv.values[i]
}

func (fv *fieldValue) Less(i, j int) bool {
	return fv.fields[i] < fv.fields[j]
}

type hasIsZero interface {
	IsZero() bool
}

// func marshal(v interface{}) (interface{}, error) {
// 	// TODO: review db.Marshaler, we may want to keep this too, etc......

// 	// if m, isMarshaler := v.(db.Marshaler); isMarshaler {
// 	// 	var err error
// 	// 	if v, err = m.MarshalDB(); err != nil {
// 	// 		return nil, err
// 	// 	}
// 	// }
// 	return v, nil
// }
