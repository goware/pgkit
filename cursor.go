package pgkit

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
)

// ErrInvalidCursor signals a client-supplied cursor that failed to decode — map to 400, not 500.
var ErrInvalidCursor = errors.New("invalid cursor")

// EncodeCursor produces an opaque cursor: base64-JSON, not signed, never use it for authorization.
func EncodeCursor[C any](cursor C) (string, error) {
	raw, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("marshal cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// DecodeCursor returns (nil, nil) for empty input so callers can compose with a nil-check.
func DecodeCursor[C any](value string) (*C, error) {
	if value == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, ErrInvalidCursor
	}
	var cursor C
	if err := json.Unmarshal(raw, &cursor); err != nil {
		return nil, ErrInvalidCursor
	}
	return &cursor, nil
}

// Cursor is the interface a typed keyset cursor satisfies — mirrors pgkit.Record[T, I]'s self-pointer pattern.
type Cursor[Self any, Row any] interface {
	*Self
	Apply(sq.SelectBuilder) sq.SelectBuilder
	From(Row) error
}

// CursorPaginator is the keyset sibling of Paginator[T] for ordering-stable pagination under concurrent writes.
// The caller owns ORDER BY; C.Apply must match it or pages will silently skip or duplicate rows.
type CursorPaginator[T any, C any, PC Cursor[C, T]] struct {
	settings PaginatorSettings
}

// NewCursorPaginator honors only size options — WithSort / WithColumnFunc are no-ops because the caller owns ORDER BY.
func NewCursorPaginator[T any, C any, PC Cursor[C, T]](options ...PaginatorOption) CursorPaginator[T, C, PC] {
	settings := &PaginatorSettings{
		DefaultSize: DefaultPageSize,
		MaxSize:     MaxPageSize,
	}
	for _, option := range options {
		option(settings)
	}
	if settings.MaxSize < settings.DefaultSize {
		settings.MaxSize = settings.DefaultSize
	}
	return CursorPaginator[T, C, PC]{settings: *settings}
}

// PrepareQuery chains LIMIT n+1 so PrepareResult can detect a next page without a second round-trip.
func (p CursorPaginator[T, C, PC]) PrepareQuery(q sq.SelectBuilder, page *Page) ([]T, sq.SelectBuilder, error) {
	if page == nil {
		page = &Page{}
	}
	page.SetDefaults(&p.settings)

	if page.Cursor != "" {
		cursor, err := DecodeCursor[C](page.Cursor)
		if err != nil {
			return nil, q, err
		}
		q = PC(cursor).Apply(q)
	}

	limit := page.Limit()
	q = q.Limit(limit + 1)
	return make([]T, 0, limit+1), q, nil
}

// PrepareResult must be called after GetAll to populate page.More and page.NextCursor.
func (p CursorPaginator[T, C, PC]) PrepareResult(result []T, page *Page) ([]T, error) {
	limit := int(page.Limit())
	page.More = len(result) > limit
	if page.More {
		result = result[:limit]
		var cursor C
		if err := PC(&cursor).From(result[len(result)-1]); err != nil {
			return nil, fmt.Errorf("cursor from row: %w", err)
		}
		next, err := EncodeCursor(cursor)
		if err != nil {
			return nil, err
		}
		page.NextCursor = next
	}
	page.Size = uint32(limit)
	return result, nil
}
