package pgkit

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
)

// ID is a comparable type used for record IDs.
type ID comparable

// Records must be a pointer with the methods defined on the pointer.
type Record[T any, I ID] interface {
	*T // Enforce T is a pointer.
	GetID() I
	Validate() error
}

// Table provides basic CRUD operations for database records.
// NOTICE: Experimental. Table and its methods are subject to change.
type Table[T any, P Record[T, I], I ID] struct {
	*DB
	Name      string
	IDColumn  string
	Paginator Paginator[P]
}

// HasSetCreatedAt is implemented by records that track creation time.
// Insert will automatically call SetCreatedAt with the current UTC time.
type HasSetCreatedAt interface {
	SetCreatedAt(time.Time)
}

// HasSetUpdatedAt is implemented by records that track update time.
// Insert, Update, and Save will automatically call SetUpdatedAt with the current UTC time.
type HasSetUpdatedAt interface {
	SetUpdatedAt(time.Time)
}

// HasSetDeletedAt is implemented by records that support soft delete.
// DeleteByID will call SetDeletedAt with the current UTC time to soft-delete,
// and RestoreByID will call SetDeletedAt with a zero time.Time{} to restore.
//
// Implementations should treat a zero time as a restore (clear the timestamp):
//
//	func (r *MyRecord) SetDeletedAt(t time.Time) {
//		if t.IsZero() {
//			r.DeletedAt = nil // restore: clear the timestamp
//			return
//		}
//		r.DeletedAt = &t // soft delete: set the timestamp
//	}
type HasSetDeletedAt interface {
	SetDeletedAt(time.Time)
}

// Insert inserts one or more records. Sets CreatedAt and UpdatedAt timestamps if available.
// Records are returned with their generated fields populated via RETURNING *.
func (t *Table[T, P, I]) Insert(ctx context.Context, records ...P) error {
	switch len(records) {
	case 0:
		return nil
	case 1:
		return t.insertOne(ctx, records[0])
	default:
		return t.insertAll(ctx, records)
	}
}

func (t *Table[T, P, I]) insertOne(ctx context.Context, record P) error {
	if record == nil {
		return fmt.Errorf("record is nil")
	}

	if err := record.Validate(); err != nil {
		return fmt.Errorf("validate record: %w", err)
	}

	now := time.Now().UTC()
	if row, ok := any(record).(HasSetCreatedAt); ok {
		row.SetCreatedAt(now)
	}
	if row, ok := any(record).(HasSetUpdatedAt); ok {
		row.SetUpdatedAt(now)
	}

	q := t.SQL.
		InsertRecord(record).
		Into(t.Name).
		Suffix("RETURNING *")

	if err := t.Query.GetOne(ctx, q, record); err != nil {
		return fmt.Errorf("insert record: %w", err)
	}

	return nil
}

func (t *Table[T, P, I]) insertAll(ctx context.Context, records []P) error {
	now := time.Now().UTC()

	for i, r := range records {
		if r == nil {
			return fmt.Errorf("record with index=%d is nil", i)
		}

		if err := r.Validate(); err != nil {
			return fmt.Errorf("validate record: %w", err)
		}

		if row, ok := any(r).(HasSetCreatedAt); ok {
			row.SetCreatedAt(now)
		}
		if row, ok := any(r).(HasSetUpdatedAt); ok {
			row.SetUpdatedAt(now)
		}
	}

	for start := 0; start < len(records); start += chunkSize {
		end := min(start+chunkSize, len(records))

		chunk := records[start:end]
		q := t.SQL.
			InsertRecords(chunk).
			Into(t.Name).
			SuffixExpr(sq.Expr(" RETURNING *"))

		if err := t.Query.GetAll(ctx, q, &chunk); err != nil {
			return fmt.Errorf("insert records: %w", err)
		}
	}

	return nil
}

// Upsert inserts records with ON CONFLICT handling. conflictColumns specify the unique
// constraint columns. If updateColumns is nil or empty, uses DO NOTHING. Otherwise,
// uses DO UPDATE SET to update the specified columns on conflict.
//
// Does not use RETURNING — with DO NOTHING the conflicting row may not be returned.
func (t *Table[T, P, I]) Upsert(ctx context.Context, conflictColumns []string, updateColumns []string, records ...P) error {
	if len(records) == 0 {
		return nil
	}
	if len(conflictColumns) == 0 {
		return fmt.Errorf("upsert: conflictColumns must not be empty")
	}

	now := time.Now().UTC()
	for i, r := range records {
		if r == nil {
			return fmt.Errorf("record with index=%d is nil", i)
		}
		if err := r.Validate(); err != nil {
			return fmt.Errorf("validate record: %w", err)
		}
		if row, ok := any(r).(HasSetCreatedAt); ok {
			row.SetCreatedAt(now)
		}
		if row, ok := any(r).(HasSetUpdatedAt); ok {
			row.SetUpdatedAt(now)
		}
	}

	// Validate column names against the first record's mapped columns.
	cols, _, err := Map(records[0])
	if err != nil {
		return fmt.Errorf("upsert: map record: %w", err)
	}
	colSet := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		colSet[c] = struct{}{}
	}
	for _, c := range conflictColumns {
		if _, ok := colSet[c]; !ok {
			return fmt.Errorf("upsert: invalid conflict column %q", c)
		}
	}
	for _, c := range updateColumns {
		if _, ok := colSet[c]; !ok {
			return fmt.Errorf("upsert: invalid update column %q", c)
		}
	}

	// Build ON CONFLICT suffix.
	var suffix string
	if len(updateColumns) == 0 {
		suffix = fmt.Sprintf("ON CONFLICT (%s) DO NOTHING", strings.Join(conflictColumns, ", "))
	} else {
		sets := make([]string, len(updateColumns))
		for i, c := range updateColumns {
			sets[i] = fmt.Sprintf("%s = EXCLUDED.%s", c, c)
		}
		suffix = fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s", strings.Join(conflictColumns, ", "), strings.Join(sets, ", "))
	}

	for start := 0; start < len(records); start += chunkSize {
		end := min(start+chunkSize, len(records))
		chunk := records[start:end]

		q := t.SQL.InsertRecords(chunk).Into(t.Name).SuffixExpr(sq.Expr(suffix))
		if _, err := t.Query.Exec(ctx, q); err != nil {
			return fmt.Errorf("upsert records: %w", err)
		}
	}

	return nil
}

// Update updates one or more records by their ID. Sets UpdatedAt timestamp if available.
// Returns (true, nil) if at least one row was updated, (false, nil) if no rows matched.
func (t *Table[T, P, I]) Update(ctx context.Context, records ...P) (bool, error) {
	switch len(records) {
	case 0:
		return false, nil
	case 1:
		return t.updateOne(ctx, records[0])
	default:
		return t.updateAll(ctx, records)
	}
}

func (t *Table[T, P, I]) updateOne(ctx context.Context, record P) (bool, error) {
	if record == nil {
		return false, fmt.Errorf("record is nil")
	}

	var zero I
	if record.GetID() == zero {
		return false, fmt.Errorf("update record: ID is zero")
	}

	if err := record.Validate(); err != nil {
		return false, fmt.Errorf("validate record: %w", err)
	}

	if row, ok := any(record).(HasSetUpdatedAt); ok {
		row.SetUpdatedAt(time.Now().UTC())
	}

	q := t.SQL.UpdateRecord(record, sq.Eq{t.IDColumn: record.GetID()}, t.Name)
	tag, err := t.Query.Exec(ctx, q)
	if err != nil {
		return false, fmt.Errorf("update record: %w", err)
	}

	return tag.RowsAffected() > 0, nil
}

func (t *Table[T, P, I]) updateAll(ctx context.Context, records []P) (bool, error) {
	now := time.Now().UTC()

	queries := make(Queries, 0, len(records))
	var zero I

	for i, r := range records {
		if r == nil {
			return false, fmt.Errorf("record with index=%d is nil", i)
		}

		if r.GetID() == zero {
			return false, fmt.Errorf("update record with index=%d: ID is zero", i)
		}

		if err := r.Validate(); err != nil {
			return false, fmt.Errorf("validate record: %w", err)
		}

		if row, ok := any(r).(HasSetUpdatedAt); ok {
			row.SetUpdatedAt(now)
		}

		queries.Add(t.SQL.
			UpdateRecord(r, sq.Eq{t.IDColumn: r.GetID()}, t.Name).
			SuffixExpr(sq.Expr(" RETURNING *")),
		)
	}

	var affected bool
	for chunk := range slices.Chunk(queries, chunkSize) {
		tags, err := t.Query.BatchExec(ctx, chunk)
		if err != nil {
			return false, fmt.Errorf("update records: %w", err)
		}
		for _, tag := range tags {
			if tag.RowsAffected() > 0 {
				affected = true
			}
		}
	}

	return affected, nil
}

// Save inserts or updates given records. Auto-detects insert vs update by ID based on zerovalue of ID from GetID() method on record.
func (t *Table[T, P, I]) Save(ctx context.Context, records ...P) error {
	switch len(records) {
	case 0:
		return nil
	case 1:
		return t.saveOne(ctx, records[0])
	default:
		return t.saveAll(ctx, records)
	}
}

func (t *Table[T, P, I]) saveOne(ctx context.Context, record P) error {
	if record == nil {
		return fmt.Errorf("record is nil")
	}

	if err := record.Validate(); err != nil {
		return fmt.Errorf("validate record: %w", err)
	}

	now := time.Now().UTC()
	if row, ok := any(record).(HasSetUpdatedAt); ok {
		row.SetUpdatedAt(now)
	}

	// Insert
	var zero I
	if record.GetID() == zero {
		if row, ok := any(record).(HasSetCreatedAt); ok {
			row.SetCreatedAt(now)
		}

		q := t.SQL.
			InsertRecord(record).
			Into(t.Name).
			Suffix("RETURNING *")

		if err := t.Query.GetOne(ctx, q, record); err != nil {
			return fmt.Errorf("save: insert record: %w", err)
		}

		return nil
	}

	// Update
	q := t.SQL.UpdateRecord(record, sq.Eq{t.IDColumn: record.GetID()}, t.Name)
	if _, err := t.Query.Exec(ctx, q); err != nil {
		return fmt.Errorf("save: update record: %w", err)
	}

	return nil
}

const chunkSize = 1000

func (t *Table[T, P, I]) saveAll(ctx context.Context, records []P) error {
	now := time.Now().UTC()

	insertRecords := make([]P, 0)
	insertIndices := make([]int, 0) // keep track of original indices, so we can update the records with IDs in passed slice

	updateQueries := make(Queries, 0)

	for i, r := range records {
		if r == nil {
			return fmt.Errorf("record with index=%d is nil", i)
		}

		if err := r.Validate(); err != nil {
			return fmt.Errorf("validate record: %w", err)
		}

		if row, ok := any(r).(HasSetUpdatedAt); ok {
			row.SetUpdatedAt(now)
		}

		var zero I
		if r.GetID() == zero {
			if row, ok := any(r).(HasSetCreatedAt); ok {
				row.SetCreatedAt(now)
			}

			insertRecords = append(insertRecords, r)
			insertIndices = append(insertIndices, i) // remember index
		} else {
			updateQueries.Add(t.SQL.
				UpdateRecord(r, sq.Eq{t.IDColumn: r.GetID()}, t.Name).
				SuffixExpr(sq.Expr(" RETURNING *")),
			)
		}
	}

	// Handle inserts in chunks, has to be done manually, slices.Chunk does not return index :/
	for start := 0; start < len(insertRecords); start += chunkSize {
		end := min(start+chunkSize, len(insertRecords))

		chunk := insertRecords[start:end]
		q := t.SQL.
			InsertRecords(chunk).
			Into(t.Name).
			SuffixExpr(sq.Expr(" RETURNING *"))

		if err := t.Query.GetAll(ctx, q, &chunk); err != nil {
			return fmt.Errorf("insert records: %w", err)
		}

		// update original slice
		for i, rr := range chunk {
			records[insertIndices[start+i]] = rr
		}
	}

	if len(updateQueries) > 0 {
		for chunk := range slices.Chunk(updateQueries, chunkSize) {
			if _, err := t.Query.BatchExec(ctx, chunk); err != nil {
				return fmt.Errorf("update records: %w", err)
			}
		}
	}

	return nil
}

// getListQuery builds a base select query for listing records.
func (t *Table[T, P, I]) getListQuery(where sq.Sqlizer, orderBy []string) sq.SelectBuilder {
	if len(orderBy) == 0 {
		orderBy = []string{t.IDColumn}
	}

	q := t.SQL.
		Select("*").
		From(t.Name).
		Where(where).
		OrderBy(orderBy...)
	return q
}

// Get returns the first record matching the condition.
func (t *Table[T, P, I]) Get(ctx context.Context, where sq.Sqlizer, orderBy []string) (P, error) {
	record := new(T)

	q := t.getListQuery(where, orderBy).Limit(1)

	if err := t.Query.GetOne(ctx, q, record); err != nil {
		return nil, fmt.Errorf("get record: %w", err)
	}

	return record, nil
}

// List returns all records matching the condition.
func (t *Table[T, P, I]) List(ctx context.Context, where sq.Sqlizer, orderBy []string) ([]P, error) {
	q := t.getListQuery(where, orderBy)
	var records []P
	if err := t.Query.GetAll(ctx, q, &records); err != nil {
		return nil, err
	}

	return records, nil
}

// ListPaged returns paginated records matching the condition.
func (t *Table[T, P, I]) ListPaged(ctx context.Context, where sq.Sqlizer, page *Page) ([]P, *Page, error) {
	if page == nil {
		page = &Page{}
	}
	// Ensure deterministic ordering for stable pagination.
	if len(page.Sort) == 0 && page.Column == "" && len(t.Paginator.settings.Sort) == 0 {
		page.Sort = []Sort{{Column: t.IDColumn, Order: Asc}}
	}
	q := t.SQL.Select("*").From(t.Name).Where(where)

	result, q := t.Paginator.PrepareQuery(q, page)
	if err := t.Query.GetAll(ctx, q, &result); err != nil {
		return nil, nil, err
	}
	result = t.Paginator.PrepareResult(result, page)
	return result, page, nil
}

// Iter returns an iterator for records matching the condition.
func (t *Table[T, P, I]) Iter(ctx context.Context, where sq.Sqlizer, orderBy []string) (iter.Seq2[P, error], error) {
	q := t.getListQuery(where, orderBy)
	rows, err := t.Query.QueryRows(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query rows: %w", err)
	}

	return func(yield func(P, error) bool) {
		defer rows.Close()
		for rows.Next() {
			var record T
			if err := t.Query.Scan.ScanRow(&record, rows); err != nil {
				yield(nil, err)
				return
			}
			if !yield(&record, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(nil, err)
		}
	}, nil
}

// GetByID returns a record by its ID.
func (t *Table[T, P, I]) GetByID(ctx context.Context, id I) (P, error) {
	return t.Get(ctx, sq.Eq{t.IDColumn: id}, []string{t.IDColumn})
}

// ListByIDs returns records by their IDs.
func (t *Table[T, P, I]) ListByIDs(ctx context.Context, ids []I) ([]P, error) {
	return t.List(ctx, sq.Eq{t.IDColumn: ids}, nil)
}

// Count returns the number of matching records.
func (t *Table[T, P, I]) Count(ctx context.Context, where sq.Sqlizer) (uint64, error) {
	var count uint64
	q := t.SQL.
		Select("COUNT(1)").
		From(t.Name).
		Where(where)

	if err := t.Query.GetOne(ctx, q, &count); err != nil {
		return count, fmt.Errorf("count: %w", err)
	}

	return count, nil
}

// DeleteByID deletes a record by ID. Uses soft delete if .SetDeletedAt() method exists.
// Returns (true, nil) if a row was deleted, (false, nil) if no row matched.
func (t *Table[T, P, I]) DeleteByID(ctx context.Context, id I) (bool, error) {
	record, err := t.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("delete: %w", err)
	}

	// Soft delete.
	if row, ok := any(record).(HasSetDeletedAt); ok {
		row.SetDeletedAt(time.Now().UTC())
		if err := t.Save(ctx, record); err != nil {
			return false, fmt.Errorf("soft delete: %w", err)
		}
		return true, nil
	}

	// Hard delete for tables without timestamps.
	return t.HardDeleteByID(ctx, id)
}

// RestoreByID restores a soft-deleted record by ID by clearing its DeletedAt timestamp.
// Returns an error if the record does not implement .SetDeletedAt().
func (t *Table[T, P, I]) RestoreByID(ctx context.Context, id I) error {
	record, err := t.GetByID(ctx, id)
	if err != nil {
		return fmt.Errorf("restore: %w", err)
	}

	row, ok := any(record).(HasSetDeletedAt)
	if !ok {
		return fmt.Errorf("restore: record does not support soft delete")
	}

	row.SetDeletedAt(time.Time{})
	if err := t.Save(ctx, record); err != nil {
		return fmt.Errorf("restore: %w", err)
	}

	return nil
}

// HardDeleteByID permanently deletes a record by ID.
// Returns (true, nil) if a row was deleted, (false, nil) if no row matched.
func (t *Table[T, P, I]) HardDeleteByID(ctx context.Context, id I) (bool, error) {
	q := t.SQL.Delete(t.Name).Where(sq.Eq{t.IDColumn: id})
	tag, err := t.Query.Exec(ctx, q)
	if err != nil {
		return false, fmt.Errorf("hard delete: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// WithPaginator returns a table instance with the given paginator.
func (t *Table[T, P, I]) WithPaginator(opts ...PaginatorOption) *Table[T, P, I] {
	return &Table[T, P, I]{
		DB:        t.DB,
		Name:      t.Name,
		IDColumn:  t.IDColumn,
		Paginator: NewPaginator[P](opts...),
	}
}

// WithTx returns a table instance bound to the given transaction.
func (t *Table[T, P, I]) WithTx(tx pgx.Tx) *Table[T, P, I] {
	return &Table[T, P, I]{
		DB: &DB{
			Conn:  t.DB.Conn,
			SQL:   t.DB.SQL,
			Query: t.DB.TxQuery(tx),
		},
		Name:      t.Name,
		IDColumn:  t.IDColumn,
		Paginator: t.Paginator,
	}
}

// ClaimForUpdate locks matching rows with FOR UPDATE SKIP LOCKED, calls mutateFn
// on each record, saves all records (committing the mutation), and returns the
// mutated records for processing outside the transaction.
//
// If mutateFn returns an error, the transaction is rolled back and no records are returned.
func (t *Table[T, P, I]) ClaimForUpdate(ctx context.Context, where sq.Sqlizer, orderBy []string, limit uint64, mutateFn func(record P) error) ([]P, error) {
	var claimed []P

	claimWithTx := func(pgTx pgx.Tx) error {
		records, err := t.claimForUpdateWithTx(ctx, pgTx, where, orderBy, limit, mutateFn)
		if err != nil {
			return err
		}
		claimed = records
		return nil
	}

	// Reuse existing transaction if available.
	if t.DB.Query.Tx != nil {
		if err := claimWithTx(t.DB.Query.Tx); err != nil {
			return nil, fmt.Errorf("claim for update (with tx): %w", err)
		}
		return claimed, nil
	}

	err := pgx.BeginFunc(ctx, t.DB.Conn, func(pgTx pgx.Tx) error {
		return claimWithTx(pgTx)
	})
	if err != nil {
		return nil, fmt.Errorf("claim for update (new tx): %w", err)
	}

	return claimed, nil
}

// ClaimOneForUpdate is ClaimForUpdate with limit=1. Returns the single record
// or ErrNoRows if nothing matched.
func (t *Table[T, P, I]) ClaimOneForUpdate(ctx context.Context, where sq.Sqlizer, orderBy []string, mutateFn func(record P) error) (P, error) {
	records, err := t.ClaimForUpdate(ctx, where, orderBy, 1, mutateFn)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}
	if len(records) == 0 {
		return nil, ErrNoRows
	}
	return records[0], nil
}

func (t *Table[T, P, I]) claimForUpdateWithTx(ctx context.Context, pgTx pgx.Tx, where sq.Sqlizer, orderBy []string, limit uint64, mutateFn func(record P) error) ([]P, error) {
	if len(orderBy) == 0 {
		orderBy = []string{t.IDColumn}
	}

	q := t.SQL.
		Select("*").
		From(t.Name).
		Where(where).
		OrderBy(orderBy...).
		Limit(limit).
		Suffix("FOR UPDATE SKIP LOCKED")

	txQuery := t.DB.TxQuery(pgTx)

	var records []P
	if err := txQuery.GetAll(ctx, q, &records); err != nil {
		return nil, fmt.Errorf("select for update skip locked: %w", err)
	}

	for _, record := range records {
		if err := mutateFn(record); err != nil {
			return nil, fmt.Errorf("mutate record: %w", err)
		}
	}

	now := time.Now().UTC()
	for _, record := range records {
		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf("validate record after update: %w", err)
		}
		if row, ok := any(record).(HasSetUpdatedAt); ok {
			row.SetUpdatedAt(now)
		}
		q := t.SQL.UpdateRecord(record, sq.Eq{t.IDColumn: record.GetID()}, t.Name)
		if _, err := txQuery.Exec(ctx, q); err != nil {
			return nil, fmt.Errorf("update record: %w", err)
		}
	}

	return records, nil
}
