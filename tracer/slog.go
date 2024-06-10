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
	// replace placeholders with arguments useful for local debugging
	LogValues bool
	// enabled if non-zero value is provided
	LogSlowQueriesThreshold time.Duration

	// give client power to change each section which is being logged
	LogStart       func(ctx context.Context, query string, args []any)
	LogSlowQuery   func(ctx context.Context, query string, duration time.Duration)
	LogEnd         func(ctx context.Context, query string, duration time.Duration)
	LogFailedQuery func(ctx context.Context, query string, err error)
}

func NewSlogTracer(logger *slog.Logger, opts ...Option) *SlogTracer {
	logStart := func(ctx context.Context, query string, args []any) {
		if logger != nil {
			logger.LogAttrs(ctx, slog.LevelInfo, "query start", slog.String("query", query), slog.Any("args", args))
		}
	}

	logSlowQuery := func(ctx context.Context, query string, duration time.Duration) {
		if logger != nil {
			logger.LogAttrs(ctx, slog.LevelWarn, "slow query took", slog.Any("query", query), slog.String("duration", duration.String()))
		}
	}

	logEnd := func(ctx context.Context, query string, duration time.Duration) {
		if logger != nil {
			logger.LogAttrs(ctx, slog.LevelInfo, "query end", slog.Any("query", query), slog.String("duration", duration.String()))
		}
	}

	logFailed := func(ctx context.Context, query string, err error) {
		if logger != nil {
			logger.LogAttrs(ctx, slog.LevelError, "query failed", slog.Any("query", query), slog.String("err", err.Error()))
		}
	}

	cfg := &config{
		logAllQueries:           false,
		logFailedQueries:        false,
		logValues:               false,
		logSlowQueriesThreshold: 0,
		logStart:                logStart,
		logSlowQuery:            logSlowQuery,
		logEnd:                  logEnd,
		logFailedQuery:          logFailed,
	}

	for _, opt := range opts {
		opt.apply(cfg)
	}

	return &SlogTracer{
		LogAllQueries:           cfg.logAllQueries,
		LogFailedQueries:        cfg.logFailedQueries,
		LogValues:               cfg.logValues,
		LogSlowQueriesThreshold: cfg.logSlowQueriesThreshold,
		LogStart:                cfg.logStart,
		LogSlowQuery:            cfg.logSlowQuery,
		LogEnd:                  cfg.logEnd,
		LogFailedQuery:          cfg.logFailedQuery,
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
		s.LogStart(ctx, query, data.Args)
	}

	ctx = context.WithValue(ctx, contextKeyQueryStart, time.Now())
	ctx = context.WithValue(ctx, contextKeyQuery, query)

	return ctx
}

func (s *SlogTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	queryStart := ctx.Value(ctxKey("query_start")).(time.Time)
	query := ctx.Value(ctxKey("query")).(string)
	queryDuration := time.Since(queryStart)

	if s.LogSlowQueriesThreshold > 0 {
		if queryDuration > s.LogSlowQueriesThreshold {
			s.LogSlowQuery(ctx, query, queryDuration)
		}
	}

	if (s.LogAllQueries || isTracingEnabled(ctx)) && data.Err == nil {
		s.LogEnd(ctx, query, queryDuration)
	}

	if s.LogFailedQueries && data.Err != nil && !errors.Is(data.Err, sql.ErrNoRows) {
		s.LogFailedQuery(ctx, query, data.Err)
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
