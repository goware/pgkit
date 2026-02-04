package pgkit

import (
	"fmt"
	"regexp"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
)

const (
	// DefaultPageSize is the default number of rows per page.
	DefaultPageSize = 10
	// MaxPageSize is the maximum number of rows per page.
	MaxPageSize = 50
)

type Order string

const (
	Desc Order = "DESC"
	Asc  Order = "ASC"
)

type Sort struct {
	Column string
	Order  Order
}

func (s Sort) String() string {
	if s.Column == "" {
		return ""
	}
	s.Order = sanitizeOrder(s.Order)
	return fmt.Sprintf("%s %s", s.Column, s.Order)
}

var _MatcherOrderBy = regexp.MustCompile(`-?([a-zA-Z0-9]+)`)

func sanitizeOrder(order Order) Order {
	switch strings.ToUpper(strings.TrimSpace(string(order))) {
	case string(Desc):
		return Desc
	case string(Asc):
		return Asc
	default:
		return Asc
	}
}

func NewSort(s string) (Sort, bool) {
	s = strings.TrimSpace(s)
	if s == "" || !_MatcherOrderBy.MatchString(s) {
		return Sort{}, false
	}
	sort := Sort{
		Column: s,
		Order:  Asc,
	}
	if strings.HasPrefix(s, "-") {
		sort.Column = s[1:]
		sort.Order = Desc
	}
	return sort, true
}

type Page struct {
	Size   uint32
	Page   uint32
	More   bool
	Column string
	Sort   []Sort
}

func NewPage(size, page uint32, sort ...Sort) *Page {
	if size == 0 {
		size = DefaultPageSize
	}
	if page == 0 {
		page = 1
	}
	return &Page{
		Size: size,
		Page: page,
		Sort: sort,
	}
}

func (p *Page) GetOrder(defaultSort ...string) []Sort {
	// if page has sort, use it
	if p != nil && len(p.Sort) != 0 {
		for i, s := range p.Sort {
			s.Column = strings.TrimSpace(s.Column)
			s.Column = pgx.Identifier(strings.Split(s.Column, ".")).Sanitize()
			s.Order = sanitizeOrder(s.Order)
			p.Sort[i] = s
		}
		return p.Sort
	}
	// if page has column, use default sort
	if p == nil || p.Column == "" {
		sort := make([]Sort, 0, len(defaultSort))
		for _, s := range defaultSort {
			if s, ok := NewSort(s); ok {
				sort = append(sort, s)
			}
		}
		return sort
	}
	// use column
	sort := make([]Sort, 0)
	for _, part := range strings.Split(p.Column, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if s, ok := NewSort(part); ok {
			s.Column = pgx.Identifier(strings.Split(s.Column, ".")).Sanitize()
			sort = append(sort, s)
		}
	}
	return sort
}

func (p *Page) Offset(o *PaginatorOptions) uint64 {
	n := uint64(1)
	if p != nil && p.Page != 0 {
		n = uint64(p.Page)
	}
	if n < 1 {
		n = 1
	}
	return (n - 1) * p.Limit(o)
}

func (p *Page) Limit(o *PaginatorOptions) uint64 {
	n, maxSize := o.getDefaults()
	if p != nil && p.Size != 0 {
		n = uint64(p.Size)
	}
	if n > uint64(maxSize) {
		n = maxSize
	}
	return n
}

// NewPaginator creates a new paginator with the given options.
// Default page size is 10 and max size is 50.
func NewPaginator[T any](options *PaginatorOptions) Paginator[T] {
	p := Paginator[T]{}
	if options == nil {
		options = &PaginatorOptions{}
	}
	if options.DefaultSize == 0 {
		options.DefaultSize = DefaultPageSize
	}
	if options.MaxSize == 0 {
		options.MaxSize = MaxPageSize
	}
	p.PaginatorOptions = *options
	return p
}

type PaginatorOptions struct {
	DefaultSize uint32
	MaxSize     uint32
	Sort        []string
	ColumnFunc  func(string) string
}

func (o *PaginatorOptions) getDefaults() (defaultSize, maxSize uint64) {
	defaultSize = DefaultPageSize
	maxSize = MaxPageSize
	if o == nil {
		return
	}
	if o.DefaultSize != 0 {
		defaultSize = uint64(o.DefaultSize)
	}
	if o.MaxSize != 0 {
		maxSize = uint64(o.MaxSize)
	}
	return min(defaultSize, maxSize), maxSize
}

// Paginator is a helper to paginate results.
type Paginator[T any] struct {
	PaginatorOptions
}

func (p Paginator[T]) getOrder(page *Page) []string {
	sort := page.GetOrder(p.Sort...)
	list := make([]string, len(sort))
	for i, s := range sort {
		if p.ColumnFunc != nil {
			s.Column = p.ColumnFunc(s.Column)
		}
		list[i] = s.String()
	}
	return list
}

// PrepareQuery adds pagination to the query. It sets the number of max rows to limit+1.
func (p Paginator[T]) PrepareQuery(q sq.SelectBuilder, page *Page) ([]T, sq.SelectBuilder) {
	if page == nil {
		page = &Page{}
	}
	if page.Page == 0 {
		page.Page = 1
	}
	if page.Size == 0 {
		page.Size = p.DefaultSize
	}
	if p.MaxSize != 0 && page.Size > p.MaxSize {
		page.Size = p.MaxSize
	}

	limit := page.Limit(&p.PaginatorOptions)
	q = q.Limit(page.Limit(&p.PaginatorOptions) + 1).Offset(page.Offset(&p.PaginatorOptions)).OrderBy(p.getOrder(page)...)
	return make([]T, 0, limit+1), q
}

func (p Paginator[T]) PrepareRaw(q string, args []any, page *Page) ([]T, string, []any) {
	limit, offset := page.Limit(&p.PaginatorOptions), page.Offset(&p.PaginatorOptions)

	q = q + " ORDER BY " + strings.Join(p.getOrder(page), ", ")
	q = q + " LIMIT @limit OFFSET @offset"

	for i, arg := range args {
		if existing, ok := arg.(pgx.NamedArgs); ok {
			existing["limit"] = limit + 1
			existing["offset"] = offset
			break
		}
		if i == len(args)-1 {
			args = append(args, pgx.NamedArgs{"limit": limit + 1, "offset": offset})
		}
	}

	return make([]T, 0, limit+1), q, args
}

// PrepareResult prepares the paginated result. If the number of rows is n+1:
// - it removes the last element, returning n elements
// - it sets more to true in the page object
func (p Paginator[T]) PrepareResult(result []T, page *Page) []T {
	limit := int(page.Limit(&p.PaginatorOptions))
	page.More = len(result) > limit
	if page.More {
		result = result[:limit]
	}

	page.Size = uint32(limit)
	page.Page = 1 + uint32(page.Offset(&p.PaginatorOptions))/uint32(limit)
	return result
}
