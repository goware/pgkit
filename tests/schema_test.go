package pgkit_test

import (
	"fmt"
	"time"

	"github.com/goware/pgkit/v2/dbtype"
)

type Account struct {
	ID        int64     `db:"id,omitempty"`
	Name      string    `db:"name"`
	Disabled  bool      `db:"disabled"`
	CreatedAt time.Time `db:"created_at,omitempty"` // ,omitempty will rely on postgres DEFAULT
	UpdatedAt time.Time `db:"updated_at,omitempty"` // ,omitempty will rely on postgres DEFAULT
}

func (a *Account) DBTableName() string      { return "accounts" }
func (a *Account) GetID() int64             { return a.ID }
func (a *Account) SetUpdatedAt(t time.Time) { a.UpdatedAt = t }

func (a *Account) Validate() error {
	if a.Name == "" {
		return fmt.Errorf("name is required")
	}

	return nil
}

type Article struct {
	ID        uint64     `db:"id,omitempty"`
	Author    string     `db:"author"`
	Alias     *string    `db:"alias"`
	Content   Content    `db:"content"` // using JSONB postgres datatype
	AccountID int64      `db:"account_id"`
	CreatedAt time.Time  `db:"created_at,omitempty"` // ,omitempty will rely on postgres DEFAULT
	UpdatedAt time.Time  `db:"updated_at,omitempty"` // ,omitempty will rely on postgres DEFAULT
	DeletedAt *time.Time `db:"deleted_at"`
}

func (a *Article) GetID() uint64            { return a.ID }
func (a *Article) SetUpdatedAt(t time.Time) { a.UpdatedAt = t }
func (a *Article) SetDeletedAt(t time.Time) { a.DeletedAt = &t }

func (a *Article) Validate() error {
	if a.Author == "" {
		return fmt.Errorf("author is required")
	}

	return nil
}

type Content struct {
	Title string `json:"title"`
	Body  string `json:"body"`
	Views int64  `json:"views"`
}

type Review struct {
	ID          uint64       `db:"id,omitempty"`
	Comment     string       `db:"comment"`
	Status      ReviewStatus `db:"status"`
	Sentiment   int64        `db:"sentiment"`
	AccountID   int64        `db:"account_id"`
	ArticleID   uint64       `db:"article_id"`
	ProcessedAt *time.Time   `db:"processed_at"`
	CreatedAt   time.Time    `db:"created_at,omitempty"` // ,omitempty will rely on postgres DEFAULT
	UpdatedAt   time.Time    `db:"updated_at,omitempty"` // ,omitempty will rely on postgres DEFAULT
	DeletedAt   *time.Time   `db:"deleted_at"`
}

func (r *Review) GetID() uint64            { return r.ID }
func (r *Review) SetUpdatedAt(t time.Time) { r.UpdatedAt = t }
func (r *Review) SetDeletedAt(t time.Time) { r.DeletedAt = &t }

func (r *Review) Validate() error {
	if len(r.Comment) < 3 {
		return fmt.Errorf("comment too short")
	}

	return nil
}

type ReviewStatus int64

const (
	ReviewStatusPending ReviewStatus = iota
	ReviewStatusProcessing
	ReviewStatusApproved
	ReviewStatusRejected
	ReviewStatusFailed
)

type Log struct {
	ID      int64  `db:"id,omitempty"`
	Message string `db:"message"`
	// RawData []byte                 `db:"raw_data"`
	RawData dbtype.HexBytes        `db:"raw_data"`
	Etc     map[string]interface{} `db:"etc"` // using JSONB postgres datatype
}

type Stat struct {
	ID     int64         `db:"id,omitempty"`
	Key    string        `db:"key"`
	Num    dbtype.BigInt `db:"big_num"` // using NUMERIC(78,0) postgres datatype
	Rating dbtype.BigInt `db:"rating"`  // using NUMERIC(78,0) postgres datatype
}
