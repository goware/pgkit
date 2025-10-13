package pgkit

import (
	"context"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
)

// Table provides basic CRUD operations for database records.
// Records must implement GetID() and Validate() methods.
type Table[T any, PT interface {
	*T // Enforce T is a pointer; and thus all methods are defined on a pointer receiver.
	GetID() IDT
	Validate() error
}, IDT comparable] struct {
	*DB
	Name     string
	IDColumn string
}

type hasSetUpdatedAt interface {
	SetUpdatedAt(time.Time)
}

type hasSetDeletedAt interface {
	SetDeletedAt(time.Time)
}

// Save inserts or updates a record. Auto-detects insert vs update by ID.
func (t *Table[T, PT, IDT]) Save(ctx context.Context, record PT) error {
	if err := record.Validate(); err != nil {
		return err //nolint:wrapcheck
	}

	if row, ok := any(record).(hasSetUpdatedAt); ok {
		row.SetUpdatedAt(time.Now().UTC())
	}

	// Insert
	var zero IDT
	if record.GetID() == zero {
		q := t.SQL.InsertRecord(record).Into(t.Name).Suffix("RETURNING *")
		if err := t.Query.GetOne(ctx, q, record); err != nil {
			return fmt.Errorf("insert records: %w", err)
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

// SaveAll saves multiple records sequentially.
func (t *Table[T, PT, IDT]) SaveAll(ctx context.Context, records []PT) error {
	for i := range records {
		if err := t.Save(ctx, records[i]); err != nil {
			return err
		}
	}

	return nil
}

// GetOne returns the first record matching the condition.
func (t *Table[T, PT, IDT]) GetOne(ctx context.Context, cond sq.Sqlizer, orderBy []string) (PT, error) {
	if len(orderBy) == 0 {
		orderBy = []string{t.IDColumn}
	}

	dest := new(T)

	q := t.SQL.
		Select("*").
		From(t.Name).
		Where(cond).
		Limit(1).
		OrderBy(orderBy...)

	if err := t.Query.GetOne(ctx, q, dest); err != nil {
		return nil, err
	}

	return dest, nil
}

// GetAll returns all records matching the condition.
func (t *Table[T, PT, IDT]) GetAll(ctx context.Context, cond sq.Sqlizer, orderBy []string) ([]PT, error) {
	if len(orderBy) == 0 {
		orderBy = []string{t.IDColumn}
	}

	q := t.SQL.
		Select("*").
		From(t.Name).
		Where(cond).
		OrderBy(orderBy...)

	var dest []PT
	if err := t.Query.GetAll(ctx, q, &dest); err != nil {
		return nil, err
	}

	return dest, nil
}

// GetByID returns a record by its ID.
func (t *Table[T, PT, IDT]) GetByID(ctx context.Context, id IDT) (PT, error) {
	return t.GetOne(ctx, sq.Eq{t.IDColumn: id}, []string{t.IDColumn})
}

// GetByIDs returns records by their IDs.
func (t *Table[T, PT, IDT]) GetByIDs(ctx context.Context, ids []IDT) ([]PT, error) {
	return t.GetAll(ctx, sq.Eq{t.IDColumn: ids}, nil)
}

// Count returns the number of matching records.
func (t *Table[T, PT, IDT]) Count(ctx context.Context, cond sq.Sqlizer) (uint64, error) {
	var count uint64
	q := t.SQL.
		Select("COUNT(1)").
		From(t.Name).
		Where(cond)

	if err := t.Query.GetOne(ctx, q, &count); err != nil {
		return count, fmt.Errorf("get one: %w", err)
	}

	return count, nil
}

// DeleteByID deletes a record by ID. Uses soft delete if .SetDeletedAt() method exists.
func (t *Table[T, PT, IDT]) DeleteByID(ctx context.Context, id IDT) error {
	record, err := t.GetByID(ctx, id)
	if err != nil {
		return err
	}

	// Soft delete.
	if row, ok := any(record).(hasSetDeletedAt); ok {
		row.SetDeletedAt(time.Now().UTC())
		return t.Save(ctx, record)
	}

	// Hard delete for tables without timestamps.
	return t.HardDeleteByID(ctx, id)
}

// HardDeleteByID permanently deletes a record by ID.
func (t *Table[T, PT, IDT]) HardDeleteByID(ctx context.Context, id IDT) error {
	q := t.SQL.Delete(t.Name).Where(sq.Eq{t.IDColumn: id})
	_, err := t.Query.Exec(ctx, q)
	return err
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

// LockForUpdate locks 0..limit records using PostgreSQL's FOR UPDATE SKIP LOCKED pattern
// for safe concurrent processing where each record is processed exactly once.
// Complete updateFn() quickly to avoid holding the transaction. For long-running work:
// update status to "processing" and return early, then process asynchronously.
func (t *Table[T, PT, IDT]) LockForUpdate(ctx context.Context, cond sq.Sqlizer, orderBy []string, limit uint64, updateFn func([]PT)) error {
	return pgx.BeginFunc(ctx, t.DB.Conn, func(pgTx pgx.Tx) error {
		if len(orderBy) == 0 {
			orderBy = []string{t.IDColumn}
		}

		tx := t.WithTx(pgTx)

		q := tx.SQL.
			Select("*").
			From(t.Name).
			Where(cond).
			OrderBy(orderBy...).
			Limit(limit).
			Suffix("FOR UPDATE SKIP LOCKED")

		var records []PT
		if err := tx.Query.GetAll(ctx, q, &records); err != nil {
			return fmt.Errorf("select for update skip locked: %w", err)
		}

		updateFn(records)

		for _, record := range records {
			q := tx.SQL.UpdateRecord(record, sq.Eq{t.IDColumn: record.GetID()}, t.Name)
			if _, err := tx.Query.Exec(ctx, q); err != nil {
				return fmt.Errorf("update record: %w", err)
			}
		}

		return nil
	})
}
