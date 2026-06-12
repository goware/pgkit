package pgkit

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/lann/builder"
)

var (
	// ErrInvalidCursor signals a client-supplied cursor that failed to decode - map to 400, not 500.
	ErrInvalidCursor = errors.New("invalid cursor")
	// ErrCursorQueryOrdered signals a cursor-paginated query that already had ORDER BY.
	ErrCursorQueryOrdered = errors.New("cursor query already has order by")
	// ErrCursorPageOrdered signals page-level ordering that does not match the cursor order.
	ErrCursorPageOrdered = errors.New("cursor page order does not match cursor order")
	// ErrCursorPaged signals a page carrying both a cursor and a page number.
	ErrCursorPaged = errors.New("cursor and page number are mutually exclusive")
)

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
	PtrTo[Self]
	Apply(sq.SelectBuilder) sq.SelectBuilder
	From(Row) error
	// OrderBy must match Apply and should include a unique tiebreaker.
	OrderBy() []Sort
}

// CursorPaginator is the keyset sibling of Paginator[T] for ordering-stable pagination under concurrent writes.
type CursorPaginator[T any, C any, PC Cursor[C, T]] struct {
	settings PaginatorSettings
}

// NewCursorPaginator honors only size options - the cursor owns ORDER BY.
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

	if _, ok := builder.Get(q, "OrderByParts"); ok {
		return nil, q, ErrCursorQueryOrdered
	}
	var zero C
	order := PC(&zero).OrderBy()
	pageOrder := page.GetOrder(nil)
	if len(pageOrder) != 0 && len(pageOrder) != len(order) {
		return nil, q, ErrCursorPageOrdered
	}
	for i := range pageOrder {
		if pageOrder[i] != order[i].sanitize(nil) {
			return nil, q, ErrCursorPageOrdered
		}
	}
	for _, sort := range order {
		q = q.OrderBy(sort.String())
	}
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

// Paginate returns cursor-paginated rows and the page populated with More and NextCursor.
func (p CursorPaginator[T, C, PC]) Paginate(ctx context.Context, query *Querier, q sq.SelectBuilder, page *Page) ([]T, *Page, error) {
	if page == nil {
		page = &Page{}
	}
	result, q, err := p.PrepareQuery(q, page)
	if err != nil {
		return nil, nil, err
	}
	if err := query.GetAll(ctx, q, &result); err != nil {
		return nil, nil, err
	}
	result, err = p.PrepareResult(result, page)
	if err != nil {
		return nil, nil, err
	}
	return result, page, nil
}

// PrepareResult must be called after GetAll to populate page.More and page.NextCursor.
func (p CursorPaginator[T, C, PC]) PrepareResult(result []T, page *Page) ([]T, error) {
	limit := int(page.Limit())
	page.Size = uint32(limit)
	page.More = len(result) > limit
	page.NextCursor = ""
	if !page.More {
		return result, nil
	}
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
	return result, nil
}
