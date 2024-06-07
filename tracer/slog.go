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

type SlogTracer struct {
	Logger           *slog.Logger
	LogAllQueries    bool
	LogFailedQueries bool
	// log passed params
	// replace placeholders with arguments useful for local debugging
	LogValues bool
	// enabled if non-zero value is provided
	LogSlowQueriesThreshold time.Duration
}

func NewSlogTracer(logger *slog.Logger) *SlogTracer {
	return &SlogTracer{
		Logger:                  logger,
		LogAllQueries:           false,
		LogFailedQueries:        false,
		LogValues:               false,
		LogSlowQueriesThreshold: 0,
	}
}

func (s *SlogTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	query, _ := ctx.Value(ctxKey("query")).(string)
	if s.LogValues {
		for i, placeholder := range data.Args {
			query = strings.Replace(query, fmt.Sprintf("$%d", i+1), fmt.Sprintf("%v", placeholder), 1)
		}
	}

	if s.LogAllQueries {
		if s.LogValues {
			s.Logger.Info("query start", slog.String("sql", query), slog.Any("args", data.Args))
		} else {
			s.Logger.Info("query start", slog.String("sql", query))
		}
	}

	ctx = context.WithValue(ctx, ctxKey("query_start"), time.Now())
	ctx = context.WithValue(ctx, ctxKey("query"), query)

	return ctx
}

func (s *SlogTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	err := data.Err

	queryStart := ctx.Value(ctxKey("query_start")).(time.Time)
	query := ctx.Value(ctxKey("query")).(string)

	if s.LogSlowQueriesThreshold > 0 {
		duration := time.Since(queryStart)

		if duration > s.LogSlowQueriesThreshold {
			s.Logger.Warn("query took", slog.Any("query", query), slog.String("duration", duration.String()))
		}
	}

	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		s.Logger.Error("query failed", slog.Any("query", query), slog.String("err", err.Error()))
	}
}

func (s *SlogTracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	return ctx
}

func (s *SlogTracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {

}

func (s *SlogTracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {

}
