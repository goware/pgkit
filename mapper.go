package pgkit

import (
	"fmt"
	"reflect"

	sq "github.com/Masterminds/squirrel"
	"github.com/goware/pgkit/internal/reflectx"
)

var Mapper = reflectx.NewMapper("db")

var (
	sqlDefault = sq.Expr("DEFAULT")
)

type hasIsZero interface {
	IsZero() bool
}

// MapOptions represents options for the mapper.
type MapOptions struct {
	IncludeZeroed bool
	IncludeNil    bool
}

var defaultMapOptions = MapOptions{
	IncludeZeroed: false,
	IncludeNil:    false,
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

// Map receives a pointer to map or struct and maps it to columns and values.
func Map(item interface{}, options *MapOptions) ([]string, []interface{}, error) {
	var fv fieldValue
	if options == nil {
		options = &defaultMapOptions
	}

	itemV := reflect.ValueOf(item)
	if !itemV.IsValid() {
		return nil, nil, nil
	}

	itemT := itemV.Type()

	if itemT.Kind() == reflect.Ptr {
		// Single dereference. Just in case the user passes a pointer to struct
		// instead of a struct.
		item = itemV.Elem().Interface()
		itemV = reflect.ValueOf(item)
		itemT = itemV.Type()
	}

	switch itemT.Kind() {

	case reflect.Struct:
		fieldMap := Mapper.TypeMap(itemT).Names
		nfields := len(fieldMap)

		fv.values = make([]interface{}, 0, nfields)
		fv.fields = make([]string, 0, nfields)

		for _, fi := range fieldMap {

			// Field options
			_, tagOmitEmpty := fi.Options["omitempty"]

			fld := reflectx.FieldByIndexesReadOnly(itemV, fi.Index)
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
				if !tagOmitEmpty {
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
			v, err := marshal(value)
			if err != nil {
				return nil, nil, err
			}
			if isZero && tagOmitEmpty {
				v = sqlDefault
			}
			fv.values = append(fv.values, v)
		}

	case reflect.Map:
		nfields := itemV.Len()
		fv.values = make([]interface{}, nfields)
		fv.fields = make([]string, nfields)
		mkeys := itemV.MapKeys()

		for i, keyV := range mkeys {
			valv := itemV.MapIndex(keyV)
			fv.fields[i] = fmt.Sprintf("%v", keyV.Interface())

			v, err := marshal(valv.Interface())
			if err != nil {
				return nil, nil, err
			}

			fv.values[i] = v
		}

	default:
		return nil, nil, ErrExpectingPointerToEitherMapOrStruct
	}

	// TODO: sort or not..? it normalizes the order, but is there a benefit?
	// sort.Sort(&fv)

	return fv.fields, fv.values, nil
}

var ErrExpectingPointerToEitherMapOrStruct = fmt.Errorf(`expecting a pointer to either a map or a struct`)

func marshal(v interface{}) (interface{}, error) {
	// TODO: review db.Marshaler, we may want to keep this too, etc......

	// if m, isMarshaler := v.(db.Marshaler); isMarshaler {
	// 	var err error
	// 	if v, err = m.MarshalDB(); err != nil {
	// 		return nil, err
	// 	}
	// }
	return v, nil
}
