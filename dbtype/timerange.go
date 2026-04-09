package dbtype

import (
	"fmt"
	"time"
)

// TimeRange holds optional floor/ceil bounds for time-based ID range queries
// against a primary key column. It implements squirrel.Sqlizer, so it can be
// passed directly to squirrel.Where().
//
// Construct via [UUIDv7Range] or [Int64IDRange].
type TimeRange struct {
	column string
	floor  any
	ceil   any
}

// UUIDv7Range builds a TimeRange that brackets column with [FloorUUIDv7] / [CeilUUIDv7].
// Nil since or until leaves that side unbounded.
func UUIDv7Range(column string, since, until *time.Time) TimeRange {
	r := TimeRange{column: column}
	if since != nil {
		r.floor = FloorUUIDv7(*since).String()
	}
	if until != nil {
		r.ceil = CeilUUIDv7(*until).String()
	}
	return r
}

// Int64IDRange builds a TimeRange that brackets column with [FloorInt64ID] / [CeilInt64ID].
// Nil since or until leaves that side unbounded.
func Int64IDRange(column string, since, until *time.Time) TimeRange {
	r := TimeRange{column: column}
	if since != nil {
		r.floor = FloorInt64ID(*since)
	}
	if until != nil {
		r.ceil = CeilInt64ID(*until)
	}
	return r
}

// Floor returns the lower-bound value and true, or (nil, false) if unbounded.
func (r TimeRange) Floor() (any, bool) { return r.floor, r.floor != nil }

// Ceil returns the upper-bound value and true, or (nil, false) if unbounded.
func (r TimeRange) Ceil() (any, bool) { return r.ceil, r.ceil != nil }

// ToSql implements squirrel.Sqlizer.
// Returns "column BETWEEN ? AND ?", "column >= ?", "column <= ?",
// or "1=1" depending on which bounds are set.
func (r TimeRange) ToSql() (string, []any, error) {
	switch {
	case r.floor != nil && r.ceil != nil:
		return fmt.Sprintf("%s BETWEEN ? AND ?", r.column), []any{r.floor, r.ceil}, nil
	case r.floor != nil:
		return fmt.Sprintf("%s >= ?", r.column), []any{r.floor}, nil
	case r.ceil != nil:
		return fmt.Sprintf("%s <= ?", r.column), []any{r.ceil}, nil
	default:
		return "1=1", nil, nil
	}
}
