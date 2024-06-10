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
	"github.com/jackc/pgx/v5/pgconn"
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

func NewSlogTracer(logger *slog.Logger, opts ...Option) *SlogTracer {
	cfg := &config{
		logAllQueries:           false,
		logFailedQueries:        false,
		logValues:               false,
		logSlowQueriesThreshold: 0,
	}

	for _, opt := range opts {
		opt.apply(cfg)
	}

	return &SlogTracer{
		Logger:                  logger,
		LogAllQueries:           cfg.logAllQueries,
		LogFailedQueries:        cfg.logFailedQueries,
		LogValues:               cfg.logValues,
		LogSlowQueriesThreshold: cfg.logSlowQueriesThreshold,
	}
}

func (s *SlogTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	query := data.SQL
	if s.LogValues {
		for i, placeholder := range data.Args {
			query = strings.Replace(query, fmt.Sprintf("$%d", i+1), fmt.Sprintf("%v", placeholder), 1)
		}
	}

	if s.LogAllQueries || isTracingEnabled(ctx) {
		if s.LogValues {
			s.Logger.Info("query start", slog.String("query", query), slog.Any("args", data.Args))
		} else {
			s.Logger.Info("query start", slog.String("query", query))
		}
	}

	ctx = context.WithValue(ctx, contextKeyQueryStart, time.Now())
	ctx = context.WithValue(ctx, contextKeyQuery, query)

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

	if s.LogFailedQueries || isTracingEnabled(ctx) {
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			s.Logger.Error("query failed", slog.Any("query", query), slog.String("err", err.Error()))
		}
	}
}

func (s *SlogTracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	query := data.Batch.QueuedQueries[0]

	return s.TraceQueryStart(ctx, conn, pgx.TraceQueryStartData{
		SQL:  query.SQL,
		Args: query.Arguments,
	})
}

func (s *SlogTracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	// do nothing
}

func (s *SlogTracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	s.TraceQueryEnd(ctx, conn, pgx.TraceQueryEndData{
		CommandTag: pgconn.CommandTag{},
		Err:        data.Err,
	})
}
