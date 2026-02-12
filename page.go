package pgkit

import (
	"fmt"
	"regexp"
	"slices"
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

func (s Sort) sanitize(columnFunc func(string) string) Sort {
	s.Column = strings.TrimSpace(s.Column)
	if s.Column != "" {
		if columnFunc != nil {
			s.Column = columnFunc(s.Column)
		}
		s.Column = pgx.Identifier(strings.Split(s.Column, ".")).Sanitize()
	}

	switch strings.ToUpper(strings.TrimSpace(string(s.Order))) {
	case string(Desc):
		s.Order = Desc
	case string(Asc):
		s.Order = Asc
	default:
		s.Order = Asc
	}
	return s
}

func (s Sort) String() string {
	s = s.sanitize(nil)
	if s.Column == "" {
		return ""
	}
	return fmt.Sprintf("%s %s", s.Column, s.Order)
}

var _MatcherOrderBy = regexp.MustCompile(`-?([a-zA-Z0-9]+)`)

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
	return &Page{
		Size: size,
		Page: page,
		Sort: sort,
	}
}

func (p *Page) SetDefaults(o *PaginatorSettings) {
	if o == nil {
		o = &PaginatorSettings{
			DefaultSize: DefaultPageSize,
			MaxSize:     MaxPageSize,
		}
	}
	if p.Size == 0 {
		p.Size = o.DefaultSize
	}
	if p.Size > o.MaxSize {
		p.Size = o.MaxSize
	}
	if p.Page == 0 {
		p.Page = 1
	}
}

func (p *Page) GetOrder(columnFunc func(string) string, defaultSort ...string) []Sort {
	var sorts []Sort
	if p != nil && len(p.Sort) != 0 {
		sorts = slices.Clone(p.Sort)
	}
	// fall back to column
	if len(sorts) == 0 {
		if p != nil && p.Column != "" {
			for part := range strings.SplitSeq(p.Column, ",") {
				if s, ok := NewSort(part); ok {
					sorts = append(sorts, s)
				}
			}
		}
	}
	if len(sorts) == 0 {
		for _, s := range defaultSort {
			if s, ok := NewSort(s); ok {
				sorts = append(sorts, s)
			}
		}
	}

	for i := range sorts {
		sorts[i] = sorts[i].sanitize(columnFunc)
	}
	return sorts
}

func (p *Page) Offset() uint64 {
	n := uint64(1)
	if p != nil && p.Page != 0 {
		n = uint64(p.Page)
	}
	if n < 1 {
		n = 1
	}
	return (n - 1) * p.Limit()
}

func (p *Page) Limit() uint64 {
	n := uint64(DefaultPageSize)
	if p != nil && p.Size != 0 {
		n = uint64(p.Size)
	}
	return n
}

// PaginatorSettings are the settings for the paginator.
type PaginatorSettings struct {
	// DefaultSize is the default number of rows per page.
	// If zero, DefaultPageSize is used.
	DefaultSize uint32

	// MaxSize is the maximum number of rows per page.
	// If zero, MaxPageSize is used. If less than DefaultSize, it is set to DefaultSize.
	MaxSize uint32

	// Sort is the default sort order.
	Sort []string

	// ColumnFunc is a transformation applied to  column names.
	ColumnFunc func(string) string
}

type PaginatorOption func(*PaginatorSettings)

// WithDefaultSize sets the default page size.
func WithDefaultSize(size uint32) PaginatorOption {
	return func(s *PaginatorSettings) {
		s.DefaultSize = size
	}
}

// WithMaxSize sets the maximum page size.
func WithMaxSize(size uint32) PaginatorOption {
	return func(s *PaginatorSettings) {
		s.MaxSize = size
	}
}

// WithSort sets the default sort order.
func WithSort(sort ...string) PaginatorOption {
	return func(s *PaginatorSettings) {
		s.Sort = sort
	}
}

// WithColumnFunc sets a function to transform column names.
func WithColumnFunc(f func(string) string) PaginatorOption {
	return func(s *PaginatorSettings) {
		s.ColumnFunc = f
	}
}

// NewPaginator creates a new paginator with the given options.
// If MaxSize is less than DefaultSize, MaxSize is set to DefaultSize.
func NewPaginator[T any](options ...PaginatorOption) Paginator[T] {
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
	return Paginator[T]{settings: *settings}
}

// Paginator is a helper to paginate results.
type Paginator[T any] struct {
	settings PaginatorSettings
}

func (p Paginator[T]) getOrder(page *Page) []string {
	sort := page.GetOrder(p.settings.ColumnFunc, p.settings.Sort...)
	list := make([]string, len(sort))
	for i := range sort {
		list[i] = fmt.Sprintf("%s %s", sort[i].Column, sort[i].Order)
	}
	return list
}

// PrepareQuery adds pagination to the query. It sets the number of max rows to limit+1.
func (p Paginator[T]) PrepareQuery(q sq.SelectBuilder, page *Page) ([]T, sq.SelectBuilder) {
	if page == nil {
		page = &Page{}
	}
	page.SetDefaults(&p.settings)

	limit := page.Limit()
	q = q.Limit(limit + 1).Offset(page.Offset()).OrderBy(p.getOrder(page)...)
	return make([]T, 0, limit+1), q
}

func (p Paginator[T]) PrepareRaw(q string, args []any, page *Page) ([]T, string, []any) {
	if page == nil {
		page = &Page{}
	}
	page.SetDefaults(&p.settings)

	limit, offset := page.Limit(), page.Offset()

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
	limit := int(page.Limit())
	page.More = len(result) > limit
	if page.More {
		result = result[:limit]
	}

	page.Size = uint32(limit)
	page.Page = 1 + uint32(page.Offset())/uint32(limit)
	return result
}
