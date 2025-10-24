package pgkit

import (
	"context"
	"fmt"
	"slices"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
)

// NewTable creates a new Table instance for a specific record type.
//
// T: the concrete struct type representing a single record (e.g., Account).
// PT: the pointer type to T (e.g., *Account), which must implement tableP[T, IDT].
// IDT: the type of the primary key (e.g., int64), which must be comparable.
//
// db: a pointer to the DB instance
//
// The function introspects a zero value of T to extract metadata if it
// implements the Base[IDT], specifically:
//
//   - Validate() error -> validation function, which si called everytime on saving
//   - DBTableName() string -> returns the database table name.
//   - GetID() IDT -> returns primary key ID
//   - GetIDColumn() string -> returns the name of the primary key column, which is then used for default sorting
//
// Additionally, records may optionally implement the following interfaces to
// allow automatic timestamp management:
//
//   - hasSetCreatedAt -> SetCreatedAt(time.Time), called automatically on record creation
//   - hasSetUpdatedAt -> SetUpdatedAt(time.Time), called automatically on record creation and update
//   - hasSetDeletedAt -> SetDeletedAt(time.Time), called automatically on soft deletion
//
// Example usage:
//
//	type accountsTable struct {
//	    *pgkit.Table[Account, *Account, int64]
//	}
//
//	at := accountsTable{
//	    Table: pgkit.NewTable[Account, *Account, int64](db),
//	}
//
//	type Account struct {
//	    ID        int64     `db:"id,omitempty"`
//	    Name      string    `db:"name"`
//	    CreatedAt time.Time `db:"created_at,omitempty"` // ,omitempty will rely on Postgres DEFAULT
//	    UpdatedAt time.Time `db:"updated_at,omitempty"` // ,omitempty will rely on Postgres DEFAULT
//	}
//
//	func (a *Account) DBTableName() string      { return "accounts" }
//	func (a *Account) GetIDColumn() string      { return "id" }
//	func (a *Account) GetID() int64             { return a.ID }
//	func (a *Account) SetUpdatedAt(t time.Time) { a.UpdatedAt = t }
//
//	func (a *Account) Validate() error {
//	    if a.Name == "" {
//	        return fmt.Errorf("name is required")
//	    }
//	    return nil
//	}
func NewTable[T any, PT TableP[T, IDT], IDT comparable](db *DB, name string) *Table[T, PT, IDT] {
	var t T

	idColumn := ""
	if v, ok := any(&t).(Base[IDT]); ok {
		idColumn = v.GetIDColumn()
	}

	return &Table[T, PT, IDT]{
		DB:       db,
		Name:     name,
		IDColumn: idColumn,
	}
}

// Table provides basic CRUD operations for database records.
// Records must implement GetID() and Validate() methods.
type Table[T any, PT TableP[T, IDT], IDT comparable] struct {
	*DB
	Name     string
	IDColumn string
}

type TableP[T any, IDT comparable] interface {
	*T // Enforce that T is a pointer.
	Base[IDT]
}

type Base[IDT comparable] interface {
	Validate() error

	GetID() IDT
	GetIDColumn() string
}

func (t *Table[T, PT, IDT]) DBTableName() string { return t.Name }

type hasSetCreatedAt interface {
	SetCreatedAt(time.Time)
}

type hasSetUpdatedAt interface {
	SetUpdatedAt(time.Time)
}

type hasSetDeletedAt interface {
	SetDeletedAt(time.Time)
}

// Save inserts or updates given records. Auto-detects insert vs update by ID based on zerovalue of ID from GetID() method on record.
func (t *Table[T, PT, IDT]) Save(ctx context.Context, records ...PT) error {
	switch len(records) {
	case 0:
		return nil
	case 1:
		return t.saveOne(ctx, records[0])
	default:
		return t.saveAll(ctx, records)
	}
}

func (t *Table[T, PT, IDT]) saveOne(ctx context.Context, record PT) error {
	if record == nil {
		return fmt.Errorf("record is nil")
	}

	if err := record.Validate(); err != nil {
		return fmt.Errorf("validate record: %w", err)
	}

	if row, ok := any(record).(hasSetUpdatedAt); ok {
		row.SetUpdatedAt(time.Now().UTC())
	}

	// Insert
	var zero IDT
	if record.GetID() == zero {
		q := t.SQL.
			InsertRecord(record).
			Into(t.Name).
			Suffix("RETURNING *")

		if err := t.Query.GetOne(ctx, q, record); err != nil {
			return fmt.Errorf("insert record: %w", err)
		}

		return nil
	}

	// Update
	q := t.SQL.UpdateRecord(record, sq.Eq{t.IDColumn: record.GetID()}, t.Name)
	if _, err := t.Query.Exec(ctx, q); err != nil {
		return fmt.Errorf("update record: %w", err)
	}

	return nil
}

const chunkSize = 1000

func (t *Table[T, PT, IDT]) saveAll(ctx context.Context, records []PT) error {
	now := time.Now().UTC()

	insertRecords := make([]PT, 0)
	insertIndices := make([]int, 0) // keep track of original indices, so we can update the records with IDs in passed slice

	updateQueries := make(Queries, 0)

	for i, r := range records {
		if r == nil {
			return fmt.Errorf("record with index=%d is nil", i)
		}

		if err := r.Validate(); err != nil {
			return fmt.Errorf("validate record: %w", err)
		}

		if row, ok := any(r).(hasSetUpdatedAt); ok {
			row.SetUpdatedAt(now)
		}

		var zero IDT
		if r.GetID() == zero {
			if row, ok := any(r).(hasSetCreatedAt); ok {
				row.SetCreatedAt(now)
			}

			insertRecords = append(insertRecords, r)
			insertIndices = append(insertIndices, i) // remember index
		} else {
			updateQueries.Add(t.SQL.
				UpdateRecord(r, sq.Eq{"id": r.GetID()}, t.Name).
				SuffixExpr(sq.Expr(" RETURNING *")),
			)
		}
	}

	// Handle inserts in chunks, has to be done manually, slices.Chunk does not return index :/
	for start := 0; start < len(insertRecords); start += chunkSize {
		end := start + chunkSize
		if end > len(insertRecords) {
			end = len(insertRecords)
		}

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

// Get returns the first record matching the condition.
func (t *Table[T, PT, IDT]) Get(ctx context.Context, where sq.Sqlizer, orderBy []string) (PT, error) {
	if len(orderBy) == 0 {
		orderBy = []string{t.IDColumn}
	}

	record := new(T)

	q := t.SQL.
		Select("*").
		From(t.Name).
		Where(where).
		Limit(1).
		OrderBy(orderBy...)

	if err := t.Query.GetOne(ctx, q, record); err != nil {
		return nil, fmt.Errorf("get record: %w", err)
	}

	return record, nil
}

// List returns all records matching the condition.
func (t *Table[T, PT, IDT]) List(ctx context.Context, where sq.Sqlizer, orderBy []string) ([]PT, error) {
	if len(orderBy) == 0 {
		orderBy = []string{t.IDColumn}
	}

	q := t.SQL.
		Select("*").
		From(t.Name).
		Where(where).
		OrderBy(orderBy...)

	var records []PT
	if err := t.Query.GetAll(ctx, q, &records); err != nil {
		return nil, err
	}

	return records, nil
}

// GetByID returns a record by its ID.
func (t *Table[T, PT, IDT]) GetByID(ctx context.Context, id IDT) (PT, error) {
	return t.Get(ctx, sq.Eq{t.IDColumn: id}, []string{t.IDColumn})
}

// ListByIDs returns records by their IDs.
func (t *Table[T, PT, IDT]) ListByIDs(ctx context.Context, ids []IDT) ([]PT, error) {
	return t.List(ctx, sq.Eq{t.IDColumn: ids}, nil)
}

// Count returns the number of matching records.
func (t *Table[T, PT, IDT]) Count(ctx context.Context, where sq.Sqlizer) (uint64, error) {
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
func (t *Table[T, PT, IDT]) DeleteByID(ctx context.Context, id IDT) error {
	record, err := t.GetByID(ctx, id)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	// Soft delete.
	if row, ok := any(record).(hasSetDeletedAt); ok {
		row.SetDeletedAt(time.Now().UTC())
		if err := t.Save(ctx, record); err != nil {
			return fmt.Errorf("soft delete: %w", err)
		}
		return nil
	}

	// Hard delete for tables without timestamps.
	return t.HardDeleteByID(ctx, id)
}

// HardDeleteByID permanently deletes a record by ID.
func (t *Table[T, PT, IDT]) HardDeleteByID(ctx context.Context, id IDT) error {
	q := t.SQL.Delete(t.Name).Where(sq.Eq{t.IDColumn: id})
	if _, err := t.Query.Exec(ctx, q); err != nil {
		return fmt.Errorf("hard delete: %w", err)
	}
	return nil
}

// WithTx returns a table instance bound to the given transaction.
func (t *Table[T, PT, IDT]) WithTx(tx pgx.Tx) *Table[T, PT, IDT] {
	return &Table[T, PT, IDT]{
		DB: &DB{
			Conn:  t.DB.Conn,
			SQL:   t.DB.SQL,
			Query: t.DB.TxQuery(tx),
		},
		Name: t.Name,
	}
}

// LockForUpdate locks and updates one record using PostgreSQL's FOR UPDATE SKIP LOCKED pattern
// within a database transaction for safe concurrent processing. The record is processed exactly
// once across multiple workers. The record is automatically updated after updateFn() completes.
//
// Keep updateFn() fast to avoid holding the transaction. For long-running work, update status
// to "processing" and return early, then process asynchronously. Use defer LockForUpdate()
// to update status to "completed" or "failed".
//
// Returns ErrNoRows if no matching records are available for locking.
func (t *Table[T, PT, IDT]) LockForUpdate(ctx context.Context, where sq.Sqlizer, orderBy []string, updateFn func(record PT)) error {
	var noRows bool

	err := t.LockForUpdates(ctx, where, orderBy, 1, func(records []PT) {
		if len(records) > 0 {
			updateFn(records[0])
		} else {
			noRows = true
		}
	})
	if err != nil {
		return err //nolint:wrapcheck
	}

	if noRows {
		return ErrNoRows
	}

	return nil
}

// LockForUpdates locks and updates records using PostgreSQL's FOR UPDATE SKIP LOCKED pattern
// within a database transaction for safe concurrent processing. Each record is processed exactly
// once across multiple workers. Records are automatically updated after updateFn() completes.
//
// Keep updateFn() fast to avoid holding the transaction. For long-running work, update status
// to "processing" and return early, then process asynchronously. Use defer LockForUpdate()
// to update status to "completed" or "failed".
func (t *Table[T, PT, IDT]) LockForUpdates(ctx context.Context, where sq.Sqlizer, orderBy []string, limit uint64, updateFn func(records []PT)) error {
	// Check if we're already in a transaction
	if t.DB.Query.Tx != nil {
		if err := t.lockForUpdatesWithTx(ctx, t.DB.Query.Tx, where, orderBy, limit, updateFn); err != nil {
			return fmt.Errorf("lock for update (with tx): %w", err)
		}
	}

	return pgx.BeginFunc(ctx, t.DB.Conn, func(pgTx pgx.Tx) error {
		if err := t.lockForUpdatesWithTx(ctx, pgTx, where, orderBy, limit, updateFn); err != nil {
			return fmt.Errorf("lock for update (new tx): %w", err)
		}
		return nil
	})
}

func (t *Table[T, PT, IDT]) lockForUpdatesWithTx(ctx context.Context, pgTx pgx.Tx, where sq.Sqlizer, orderBy []string, limit uint64, updateFn func(records []PT)) error {
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

	var records []PT
	if err := txQuery.GetAll(ctx, q, &records); err != nil {
		return fmt.Errorf("select for update skip locked: %w", err)
	}

	updateFn(records)

	for _, record := range records {
		q := t.SQL.UpdateRecord(record, sq.Eq{t.IDColumn: record.GetID()}, t.Name)
		if _, err := txQuery.Exec(ctx, q); err != nil {
			return fmt.Errorf("update record: %w", err)
		}
	}

	return nil
}
