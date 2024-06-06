package tracer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type ctxKey string

type Tracer struct {
	Logger          *slog.Logger
	LogSqlStatement bool
	// replace placeholders with arguments
	ReplacePlaceholders bool
	// maybe we should not log the parameters on production because of GDPR ??
	IncludeParams bool
}

func (t *Tracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	query := data.SQL
	if t.IncludeParams && t.ReplacePlaceholders {
		for i, placeholder := range data.Args {
			query = strings.Replace(query, fmt.Sprintf("$%d", i+1), fmt.Sprintf("%v", placeholder), 1)
		}
	}

	if t.LogSqlStatement {
		if t.IncludeParams {
			t.Logger.Info("query start", slog.String("sql", query), slog.Any("args", data.Args))
		} else {
			t.Logger.Info("query start", slog.String("sql", query))
		}
	}

	ctx = context.WithValue(ctx, ctxKey("query_start"), time.Now())
	ctx = context.WithValue(ctx, ctxKey("query"), query)

	return ctx
}

func (t *Tracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	err := data.Err
	queryStart, ok := ctx.Value(ctxKey("query_start")).(time.Time)
	query := ctx.Value(ctxKey("query"))

	if ok {
		end := time.Now()
		t.Logger.Info("query finished", slog.Any("query", query), slog.String("duration", end.Sub(queryStart).String()))
	}

	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Logger.Error("query failed", slog.Any("query", query), slog.String("err", err.Error()))
	}
}
