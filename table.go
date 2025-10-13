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

type hasUpdatedAt interface {
	SetUpdatedAt(time.Time)
}

type hasDeletedAt interface {
	SetDeletedAt(time.Time)
}

// Save inserts or updates a record. Auto-detects insert vs update by ID.
func (t *Table[T, PT, ID]) Save(ctx context.Context, record PT) error {
	if err := record.Validate(); err != nil {
		return err //nolint:wrapcheck
	}

	if row, ok := any(record).(hasUpdatedAt); ok {
		row.SetUpdatedAt(time.Now().UTC())
	}

	// Insert
	var zero ID
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
func (t *Table[T, PT, ID]) SaveAll(ctx context.Context, records []PT) error {
	for _, record := range records {
		if err := t.Save(ctx, record); err != nil {
			return err
		}
	}

	return nil
}

// GetOne returns the first record matching the condition.
func (t *Table[T, PT, ID]) GetOne(ctx context.Context, cond sq.Sqlizer, orderBy []string) (PT, error) {
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
func (t *Table[T, PT, ID]) GetAll(ctx context.Context, cond sq.Sqlizer, orderBy []string) ([]PT, error) {
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
func (t *Table[T, PT, ID]) GetByID(ctx context.Context, id uint64) (PT, error) {
	return t.GetOne(ctx, sq.Eq{t.IDColumn: id}, []string{t.IDColumn})
}

// GetByIDs returns records by their IDs.
func (t *Table[T, PT, ID]) GetByIDs(ctx context.Context, ids []uint64) ([]PT, error) {
	return t.GetAll(ctx, sq.Eq{t.IDColumn: ids}, nil)
}

// Count returns the number of matching records.
func (t *Table[T, PT, ID]) Count(ctx context.Context, cond sq.Sqlizer) (uint64, error) {
	var count uint64
	q := t.SQL.
		Select("COUNT(1)").
		From(t.Name).
		Where(cond)

	if err := t.Query.GetOne(ctx, q, &count); err != nil {
		return 0, fmt.Errorf("get one: %w", err)
	}

	return count, nil
}

// DeleteByID deletes a record by ID. Uses soft delete if deleted_at column exists.
func (t *Table[T, PT, ID]) DeleteByID(ctx context.Context, id uint64) error {
	resource, err := t.GetByID(ctx, id)
	if err != nil {
		return err
	}

	// Soft delete.
	if row, ok := any(resource).(hasDeletedAt); ok {
		row.SetDeletedAt(time.Now().UTC())
		return t.Save(ctx, resource)
	}

	// Hard delete for tables without timestamps
	return t.HardDeleteByID(ctx, id)
}

// HardDeleteByID permanently deletes a record by ID.
func (t *Table[T, PT, ID]) HardDeleteByID(ctx context.Context, id uint64) error {
	_, err := t.SQL.Delete(t.Name).Where(sq.Eq{t.IDColumn: id}).Exec()
	return err
}

// WithTx returns a table instance bound to the given transaction.
func (t *Table[T, TP, ID]) WithTx(tx pgx.Tx) *Table[T, TP, ID] {
	return &Table[T, TP, ID]{
		DB: &DB{
			Conn:  t.DB.Conn,
			SQL:   t.DB.SQL,
			Query: t.DB.TxQuery(tx),
		},
		Name: t.Name,
	}
}
