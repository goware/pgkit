package pgkit

import (
	"fmt"
	"regexp"
	"strings"

	sq "github.com/Masterminds/squirrel"
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
	if s.Order == "" {
		s.Order = Asc
	}
	return fmt.Sprintf("%s %s", s.Column, s.Order)
}

var _MatcherOrderBy = regexp.MustCompile(`-?([a-zA-Z0-9]+)`)

func NewSort(s string) (Sort, bool) {
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
		if s, ok := NewSort(part); ok {
			sort = append(sort, s)
		}
	}
	return sort
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
	var n = uint64(DefaultPageSize)
	if p != nil && p.Size != 0 {
		n = uint64(p.Size)
	}
	if n > MaxPageSize {
		n = MaxPageSize
	}
	return n
}

// PaginatorOption is a function that sets an option on a paginator.
type PaginatorOption[T any] func(*Paginator[T])

// WithDefaultSize sets the default page size.
func WithDefaultSize[T any](size uint32) PaginatorOption[T] {
	return func(p *Paginator[T]) { p.defaultSize = size }
}

// WithMaxSize sets the maximum page size.
func WithMaxSize[T any](size uint32) PaginatorOption[T] {
	return func(p *Paginator[T]) { p.maxSize = size }
}

// WithSort sets the default sort order.
func WithSort[T any](sort ...string) PaginatorOption[T] {
	return func(p *Paginator[T]) { p.defaultSort = sort }
}

// WithColumnFunc sets a function to transform column names.
func WithColumnFunc[T any](f func(string) string) PaginatorOption[T] {
	return func(p *Paginator[T]) { p.columnFunc = f }
}

// NewPaginator creates a new paginator with the given options.
// Default page size is 10 and max size is 50.
func NewPaginator[T any](options ...PaginatorOption[T]) Paginator[T] {
	p := Paginator[T]{
		defaultSize: DefaultPageSize,
		maxSize:     MaxPageSize,
	}
	for _, opt := range options {
		opt(&p)
	}
	return p
}

// Paginator is a helper to paginate results.
type Paginator[T any] struct {
	defaultSize uint32
	maxSize     uint32
	defaultSort []string
	columnFunc  func(string) string
}

func (p Paginator[T]) getOrder(page *Page) []string {
	sort := page.GetOrder(p.defaultSort...)
	list := make([]string, len(sort))
	for i, s := range sort {
		if p.columnFunc != nil {
			s.Column = p.columnFunc(s.Column)
		}
		list[i] = s.String()
	}
	return list
}

// PrepareQuery adds pagination to the query. It sets the number of max rows to limit+1.
func (p Paginator[T]) PrepareQuery(q sq.SelectBuilder, page *Page) ([]T, sq.SelectBuilder) {
	if page != nil {
		if page.Size == 0 {
			page.Size = p.defaultSize
		}
		if page.Size > p.maxSize {
			page.Size = p.maxSize
		}
	}
	limit := page.Limit()
	q = q.Limit(page.Limit() + 1).Offset(page.Offset()).OrderBy(p.getOrder(page)...)
	return make([]T, 0, limit+1), q
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
