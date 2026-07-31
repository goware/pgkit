package pgkit

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

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

// Map converts a struct to (column, value) slices using `db:""` struct tags.
//
// ,omitempty and ,omitzero (mutually exclusive) both skip zero values, but
// ,omitzero keeps non-nil empty slices/maps so a clear-to-empty UPDATE
// actually clears the column. Matches encoding/json's omitzero (Go 1.24+).
// IncludeNil surfaces nil pointers as DEFAULT under ,omitempty and as
// NULL under ,omitzero.
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

	switch recordT.Kind() {

	case reflect.Struct:
		// The db-tagged fields, their options, and the column order depend only
		// on the type, so they are computed once and cached (see planForType).
		plan := planForType(recordT)
		if plan.err != nil {
			return nil, nil, plan.err
		}

		fv.values = make([]interface{}, 0, len(plan.fields))
		fv.fields = make([]string, 0, len(plan.fields))

		for i := range plan.fields {
			pf := &plan.fields[i]

			fld := reflectx.FieldByIndexesReadOnly(recordV, pf.index)

			if fld.Kind() == reflect.Ptr && fld.IsNil() {
				if (pf.omitEmpty || pf.omitZero) && !options.IncludeNil {
					continue
				}
				fv.fields = append(fv.fields, pf.name)
				// ,omitempty preserves legacy: forced-include emits DEFAULT
				// so callers can fall back to the column's DB default. ,omitzero
				// is the strict tag: forced-include emits literal NULL so a
				// PATCH can clear a nullable column with a non-null default.
				var v any
				if pf.omitEmpty {
					v = sqlDefault
				}
				fv.values = append(fv.values, v)
				continue
			}

			value := fld.Interface()
			isEmpty, isStrictZero := zeroFlags(fld, pf.zero)
			skip := (isEmpty && pf.omitEmpty) || (isStrictZero && pf.omitZero)
			if skip && !options.IncludeZeroed {
				continue
			}

			fv.fields = append(fv.fields, pf.name)
			v := value
			if skip {
				v = sqlDefault
			}
			fv.values = append(fv.values, v)
		}

		// The plan is already in sorted column order, so the struct output is
		// too (skips preserve order); no per-call sort needed.
		return fv.fields, fv.values, nil

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

// mapPlan is the precomputed, per-type field layout Map uses for structs: the
// db-tagged fields in final (sorted) column order. Building it once per type
// lets each Map call skip the tag scan, option parsing, and column sort.
type mapPlan struct {
	fields []planField
	err    error // e.g. a field carrying both ,omitempty and ,omitzero
}

type planField struct {
	name      string
	index     []int
	zero      any // fi.Zero.Interface(), for zeroFlags comparison
	omitEmpty bool
	omitZero  bool
}

// mapPlanCache maps reflect.Type -> *mapPlan. sync.Map gives lock-free reads on
// the hot path once a type has been seen.
var mapPlanCache sync.Map

func planForType(recordT reflect.Type) *mapPlan {
	if p, ok := mapPlanCache.Load(recordT); ok {
		return p.(*mapPlan)
	}
	plan := buildPlan(recordT)
	actual, _ := mapPlanCache.LoadOrStore(recordT, plan)
	return actual.(*mapPlan)
}

func buildPlan(recordT reflect.Type) *mapPlan {
	fieldMap := Mapper.TypeMap(recordT).Names
	plan := &mapPlan{fields: make([]planField, 0, len(fieldMap))}

	for _, fi := range fieldMap {
		// Skip any fields which do not specify the `db:".."` tag.
		if !strings.Contains(string(fi.Field.Tag), dbTagPrefix) {
			continue
		}

		_, tagOmitEmpty := fi.Options["omitempty"]
		_, tagOmitZero := fi.Options["omitzero"]
		if tagOmitEmpty && tagOmitZero {
			return &mapPlan{err: fmt.Errorf("field %q has both ,omitempty and ,omitzero tags (mutually exclusive)", fi.Name)}
		}

		plan.fields = append(plan.fields, planField{
			name:      fi.Name,
			index:     fi.Index,
			zero:      fi.Zero.Interface(),
			omitEmpty: tagOmitEmpty,
			omitZero:  tagOmitZero,
		})
	}

	// Sort by column name so Map's output order matches the previous
	// implementation (which sorted every call) and stays stable for cache hits.
	sort.Slice(plan.fields, func(i, j int) bool {
		return plan.fields[i].name < plan.fields[j].name
	})
	return plan
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

// Two return values because omitempty and omitzero disagree only on
// non-nil empty slices; every other path returns both flags the same.
func zeroFlags(fld reflect.Value, fieldZero any) (isEmpty, isStrictZero bool) {
	if t, ok := fld.Interface().(hasIsZero); ok {
		if t.IsZero() {
			return true, true
		}
		return false, false
	}
	switch fld.Kind() {
	case reflect.Slice:
		if fld.IsNil() {
			return true, true
		}
		if fld.Len() == 0 {
			return true, false
		}
	case reflect.Map:
		if fld.IsNil() {
			return true, true
		}
	case reflect.Array:
		// omitempty must keep all-zero arrays of normal length; switching
		// to IsZero here would silently drop [16]byte UUIDs, [32]byte hashes.
		return fld.Len() == 0, fld.IsZero()
	default:
		if reflect.DeepEqual(fieldZero, fld.Interface()) {
			return true, true
		}
	}
	return false, false
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
